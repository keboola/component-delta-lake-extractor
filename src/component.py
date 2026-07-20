import io
import logging
import os
import time
from collections import OrderedDict

import requests
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import TableOperation
from databricks.sdk.service.sql import (
    Disposition,
    ExecuteStatementRequestOnWaitTimeout,
    Format,
    StatementState,
)
import databricks.sdk.errors as dbx_errors
import duckdb
import polars
import pyarrow as pa
import pyarrow.parquet as pq
from duckdb.duckdb import DuckDBPyConnection
from keboola.component.base import ComponentBase, sync_action
from keboola.component.dao import SupportedDataTypes, BaseType, ColumnDefinition
from keboola.component.exceptions import UserException
from keboola.component.sync_actions import SelectElement, ValidationResult, MessageType

from configuration import Configuration, AccessMethod, DataSelectionMode, AuthType

DUCK_DB_DIR = os.path.join(os.environ.get("TMPDIR", "/tmp"), "duckdb")

# (connect, read) timeout for Cloud Fetch presigned-link downloads, in seconds.
CLOUD_FETCH_TIMEOUT = (10, 300)

# Max seconds to poll a running Databricks statement before cancelling it.
# The preview sync action uses a short deadline to stay responsive; a full run allows longer.
STATEMENT_POLL_TIMEOUT = 1200
PREVIEW_POLL_TIMEOUT = 25


class Component(ComponentBase):
    def __init__(self):
        super().__init__()
        self.params = Configuration(**self.configuration.parameters)
        self._connection = None
        self._workspace_result_glob = None
        self.source_uri = self.build_source_uri()

    def run(self):
        if self.params.data_selection.mode == DataSelectionMode.workspace_query:
            self._workspace_result_glob = self._execute_workspace_query(self.params.data_selection.query)

        self._connection = self.init_connection()
        table_name = self.get_table_name()
        query = self.get_query()

        if self.params.destination.parquet_output:
            out_file = self.create_out_file_definition(f"{table_name}.parquet")
            q = f" COPY ({query}) TO '{out_file.full_path}'; "
            logging.debug(f"Running query: {q}; ")
            start = time.time()
            self._connection.execute(q)
            logging.debug(f"Query finished successfully in {time.time() - start} seconds")
            self.write_manifest(out_file)
        else:
            table_meta = self._connection.execute(f"""DESCRIBE {query};""").fetchall()
            schema = OrderedDict(
                {c[0]: ColumnDefinition(data_types=self.to_base_type(c[1])) for c in table_meta}
            )

            out_table = self.create_out_table_definition(
                f"{table_name}.csv",
                schema=schema,
                primary_key=self.params.destination.primary_key,
                incremental=self.params.destination.incremental,
                has_header=True,
            )

            try:
                q = f"COPY ({query}) TO '{out_table.full_path}' (HEADER, DELIMITER ',', FORCE_QUOTE *)"
                logging.debug(f"Running query: {q}; ")
                start = time.time()
                self._connection.execute(q)
                logging.debug(f"Query finished successfully in {time.time() - start} seconds")
            except duckdb.duckdb.ConversionException as e:
                raise UserException(f"Error during query execution: {e}")

            self.write_manifest(out_table)
        self._connection.close()

    def init_connection(self) -> DuckDBPyConnection:
        """
        Returns connection to temporary DuckDB database
        """
        os.makedirs(DUCK_DB_DIR, exist_ok=True)
        # TODO: On GCP consider changin tmp to /opt/tmp
        config = dict(
            temp_directory=DUCK_DB_DIR,
            extension_directory=os.path.join(DUCK_DB_DIR, "extensions"),
            threads=self.params.threads,
            max_memory=f"{self.params.max_memory}MB",
        )
        conn = duckdb.connect(config=config)

        # In workspace_query mode the query is executed on the Databricks warehouse and only the staged
        # result parquet is read locally, so no cloud storage secret is needed.
        if self.params.data_selection.mode != DataSelectionMode.workspace_query:
            conn.execute(self.build_connection_query())

        if not self.params.destination.preserve_insertion_order:
            conn.execute("SET preserve_insertion_order = false;").fetchall()

        return conn

    def _get_workspace_client(self) -> WorkspaceClient:
        """
        Returns a Databricks WorkspaceClient authenticated either with a personal access token (PAT)
        or with service principal (OAuth M2M) credentials, depending on the selected auth type.
        """
        if self.params.auth_type == AuthType.service_principal:
            return WorkspaceClient(
                host=self.params.unity_catalog_url,
                client_id=self.params.unity_catalog_client_id,
                client_secret=self.params.unity_catalog_client_secret,
            )
        return WorkspaceClient(host=self.params.unity_catalog_url, token=self.params.unity_catalog_token)

    def _execute_workspace_query(
        self, query: str, row_limit: int = None, poll_timeout: int = STATEMENT_POLL_TIMEOUT
    ) -> str:
        """
        Executes a SQL query on a Databricks SQL warehouse (Statement Execution API) and stages the
        result locally as parquet files using Cloud Fetch (EXTERNAL_LINKS + ARROW_STREAM).

        Returns a glob path of the staged parquet files, suitable for DuckDB `read_parquet`.
        """
        warehouse_id = self.params.data_selection.warehouse_id
        if not warehouse_id:
            raise UserException("A SQL Warehouse must be selected to run a Databricks SQL query.")
        if not query:
            raise UserException("The query must not be empty.")

        w = self._get_workspace_client()

        try:
            resp = w.statement_execution.execute_statement(
                warehouse_id=warehouse_id,
                statement=query,
                disposition=Disposition.EXTERNAL_LINKS,
                format=Format.ARROW_STREAM,
                wait_timeout="30s",
                on_wait_timeout=ExecuteStatementRequestOnWaitTimeout.CONTINUE,
                row_limit=row_limit,
            )

            statement_id = resp.statement_id
            deadline = time.time() + poll_timeout
            while resp.status and resp.status.state in (StatementState.PENDING, StatementState.RUNNING):
                if time.time() > deadline:
                    # Cancel so a hung/cold warehouse stops billing instead of running to the job timeout.
                    self._cancel_statement(w, statement_id)
                    raise UserException(
                        f"Databricks query did not finish within {poll_timeout}s and was cancelled. "
                        "Try a larger warehouse or a simpler query."
                    )
                time.sleep(2)
                resp = w.statement_execution.get_statement(statement_id)

            if not resp.status or resp.status.state != StatementState.SUCCEEDED:
                error = resp.status.error.message if resp.status and resp.status.error else "unknown error"
                raise UserException(f"Databricks query failed: {error}")
        except dbx_errors.platform.DatabricksError as e:
            raise UserException(f"Databricks query failed: {str(e)}") from e

        result_dir = os.path.join(DUCK_DB_DIR, "dbx_result")
        os.makedirs(result_dir, exist_ok=True)

        chunk_index = 0
        result = resp.result
        try:
            while result is not None:
                for link in result.external_links or []:
                    # Presigned URL - fetched WITHOUT an Authorization header, but any headers the link
                    # requires (e.g. Azure blob headers) must be forwarded as-is.
                    r = requests.get(
                        link.external_link, headers=link.http_headers or {}, timeout=CLOUD_FETCH_TIMEOUT
                    )
                    r.raise_for_status()
                    table = pa.ipc.open_stream(io.BytesIO(r.content)).read_all()
                    pq.write_table(table, os.path.join(result_dir, f"chunk_{chunk_index}.parquet"))
                    chunk_index += 1

                next_index = result.next_chunk_index
                if next_index is None:
                    break
                result = w.statement_execution.get_statement_result_chunk_n(statement_id, next_index)
        except requests.RequestException as e:
            # Never surface str(e): for an HTTP error it embeds the presigned URL incl. the Azure SAS
            # signature. Suppress the cause too (`from None`) so it can't leak via the logged traceback.
            detail = type(e).__name__
            status = getattr(getattr(e, "response", None), "status_code", None)
            if status is not None:
                detail = f"{detail} (HTTP {status})"
            raise UserException(f"Failed to download query result from Databricks ({detail}).") from None
        except dbx_errors.platform.DatabricksError as e:
            raise UserException(f"Failed to fetch query result from Databricks: {str(e)}") from e

        if chunk_index == 0:
            # Empty result: build an empty parquet from the result schema so the output table still has
            # the correct columns and types and an (empty) manifest is written downstream.
            columns = resp.manifest.schema.columns if resp.manifest and resp.manifest.schema else []
            empty = pa.table({c.name: pa.array([], type=self._arrow_type_from_column(c)) for c in columns})
            pq.write_table(empty, os.path.join(result_dir, "chunk_0.parquet"))

        return os.path.join(result_dir, "*.parquet")

    @staticmethod
    def _cancel_statement(w: WorkspaceClient, statement_id: str):
        try:
            w.statement_execution.cancel_execution(statement_id)
        except dbx_errors.platform.DatabricksError:
            logging.warning("Failed to cancel Databricks statement %s", statement_id)

    @staticmethod
    def _arrow_type_from_column(column) -> "pa.DataType":
        """Maps a Databricks result-manifest column to a pyarrow type (used for empty results)."""
        type_name = column.type_name.value if column.type_name else "STRING"
        if type_name == "DECIMAL":
            return pa.decimal128(column.type_precision or 38, column.type_scale or 0)
        return {
            "BOOLEAN": pa.bool_(),
            "BYTE": pa.int8(),
            "SHORT": pa.int16(),
            "INT": pa.int32(),
            "LONG": pa.int64(),
            "FLOAT": pa.float32(),
            "DOUBLE": pa.float64(),
            "DATE": pa.date32(),
            "TIMESTAMP": pa.timestamp("us"),
        }.get(type_name, pa.string())

    def _get_temp_credentials(self, w: WorkspaceClient):
        try:
            src = self.params.source
            table_id = w.tables.get(full_name=f"{src.catalog}.{src.schema_name}.{src.table}").table_id

            creds = w.temporary_table_credentials.generate_temporary_table_credentials(
                operation=TableOperation.READ, table_id=table_id
            )
            return creds
        except dbx_errors.platform.PermissionDenied as e:
            raise UserException(f"Permission denied: {str(e)}")

    def build_connection_query(self):
        session_token = None
        if self.params.access_method == AccessMethod.unity_catalog:
            w = self._get_workspace_client()

            temp_creds = self._get_temp_credentials(w)
            self.source_uri = temp_creds.url

            if temp_creds.aws_temp_credentials:
                self.params.provider = "s3"
                self.params.aws_region = w.metastores.summary().region
                self.params.aws_key_id = temp_creds.aws_temp_credentials.access_key_id
                self.params.aws_key_secret = temp_creds.aws_temp_credentials.secret_access_key
                session_token = temp_creds.aws_temp_credentials.session_token

            elif temp_creds.azure_user_delegation_sas:
                self.params.provider = "abs"
                try:
                    # url should always have this pattern: ...@ACCOUNT_NAME.dfs... https://docs.databricks.com/aws/en/connect/storage/azure-storage?language=Account%C2%A0key#access-azure-storage  # noqa: E501
                    self.params.abs_account_name = temp_creds.url.split("@")[1].split(".dfs")[0]
                except IndexError:
                    raise IndexError(f"Unable to extract account name from storage URL: {temp_creds.url}")
                self.params.abs_sas_token = temp_creds.azure_user_delegation_sas.sas_token

            else:
                raise UserException(
                    "Unsupported provider for Unity Catalog: only Azure Blob Storage and AWS S3 are supported."
                )

        match self.params.provider:
            case "abs":
                abs_conn_str = (
                    f"AccountName={self.params.abs_account_name};SharedAccessSignature={self.params.abs_sas_token}"
                )
                query = f"""
                        CREATE SECRET (
                            TYPE AZURE,
                            CONNECTION_STRING '{abs_conn_str}');
                        SET azure_transport_option_type = 'curl';
                        """
            case "s3":
                query = f"""
                        CREATE SECRET (
                            TYPE S3,
                            REGION '{self.params.aws_region}',
                            KEY_ID '{self.params.aws_key_id}',
                            SECRET '{self.params.aws_key_secret}' %s
                            );
                       """ % (f",SESSION_TOKEN '{session_token}'" if session_token else "")
            case "gcs":
                query = f"""
                        INSTALL httpfs;
                        CREATE SECRET (
                            TYPE GCS,
                            KEY_ID '{self.params.gcp_hmac_id}',
                            SECRET '{self.params.gcp_hmac_secret}'
                            );
                       """
            case _:
                raise UserException(f"Unknown provider: {self.params.provider}")

        return query

    def build_source_uri(self):
        match self.params.provider:
            case "abs":
                source_uri = f"az://{self.params.source.container_name}/{self.params.source.blob_name}"
            case "s3":
                source_uri = f"s3://{self.params.source.container_name}/{self.params.source.blob_name}"
            case "gcs":
                source_uri = f"gs://{self.params.source.container_name}/{self.params.source.blob_name}"
            case _:
                source_uri = None

        return source_uri

    def get_table_name(self):
        if not (self.params.destination.table_name or self.params.destination.file_name):
            parts = [
                self.params.source.container_name,
                self.params.source.blob_name,
                self.params.source.catalog,
                self.params.source.schema_name,
                self.params.source.table,
            ]
            table_name = "-".join(filter(None, parts))
        else:
            table_name = self.params.destination.table_name or self.params.destination.file_name
        # workspace_query has no source table to derive a name from; fall back to a generic name.
        return table_name or "query_result"

    def get_query(self):
        mode = self.params.data_selection.mode
        if mode == DataSelectionMode.workspace_query:
            # The user's SQL already ran on the warehouse; here we only read the staged result.
            query = f"SELECT * FROM read_parquet('{self._workspace_result_glob}')"
        elif mode == DataSelectionMode.custom_query:
            query = self.params.data_selection.query.lower().replace(
                "from in_table ", f"FROM delta_scan('{self.source_uri}')"
            )
        elif mode == DataSelectionMode.select_columns:
            query = f"""
            SELECT {", ".join(self.params.data_selection.columns)}
            FROM delta_scan('{self.source_uri}')"""
        elif mode == DataSelectionMode.all_data:
            query = f"SELECT * FROM delta_scan('{self.source_uri}')"
        else:
            raise UserException("Invalid data selection mode")

        return query

    @staticmethod
    def _is_complex_duckdb_type(normalized_dtype: str) -> bool:
        """
        True for DuckDB nested/complex types that have no scalar Keboola equivalent:
        arrays/lists (end with "[]", e.g. `INTEGER[]`, `DECIMAL(10,2)[]`) and STRUCT/MAP/LIST/UNION.
        """
        return normalized_dtype.endswith("]") or normalized_dtype.split("(")[0].strip() in (
            "STRUCT",
            "MAP",
            "LIST",
            "UNION",
        )

    @staticmethod
    def to_base_type(dtype: str) -> BaseType:
        """
        Converts a DuckDB DESCRIBE type string (e.g. "DECIMAL(10,0)", "VARCHAR(255)") into a Keboola
        BaseType, preserving the precision/scale or length only for genuine scalar types. Complex
        types (arrays/struct/map) map to STRING with no length.
        """
        base_type = Component.convert_base_types(dtype)
        normalized = dtype.strip().upper()
        base_name = normalized.split("(")[0].strip()
        length = None
        if not Component._is_complex_duckdb_type(normalized) and "(" in dtype:
            if base_type == SupportedDataTypes.NUMERIC or base_name in ("VARCHAR", "CHAR", "BPCHAR"):
                length = dtype[dtype.index("(") + 1:dtype.rindex(")")].replace(" ", "")
        return BaseType(dtype=base_type, length=length)

    @staticmethod
    def convert_base_types(dtype: str) -> SupportedDataTypes:
        normalized = dtype.strip().upper()
        # Nested/complex types (arrays, STRUCT/MAP/LIST/UNION) are serialized as text -> STRING.
        if Component._is_complex_duckdb_type(normalized):
            return SupportedDataTypes.STRING
        # DuckDB DESCRIBE returns parametrized types (e.g. "DECIMAL(10,0)"); strip the precision/scale
        # suffix so the base type matches.
        base_type = normalized.split("(")[0].strip()
        if base_type in [
            "TINYINT",
            "SMALLINT",
            "INTEGER",
            "BIGINT",
            "HUGEINT",
            "UTINYINT",
            "USMALLINT",
            "UINTEGER",
            "UBIGINT",
            "UHUGEINT",
        ]:
            return SupportedDataTypes.INTEGER
        elif base_type in ["DECIMAL", "NUMERIC"]:
            return SupportedDataTypes.NUMERIC
        elif base_type in ["REAL", "FLOAT", "DOUBLE"]:
            return SupportedDataTypes.FLOAT
        elif base_type == "BOOLEAN":
            return SupportedDataTypes.BOOLEAN
        elif base_type in ["TIMESTAMP", "TIMESTAMP WITH TIME ZONE"]:
            return SupportedDataTypes.TIMESTAMP
        elif base_type == "DATE":
            return SupportedDataTypes.DATE
        else:
            return SupportedDataTypes.STRING

    @sync_action("list_columns")
    def list_columns(self):
        self._connection = self.init_connection()

        out = self._connection.execute(f"""
        DESCRIBE
        FROM delta_scan('{self.source_uri}');
        """).fetchall()

        column_names = [SelectElement(c[0], f"{c[0]} ({c[1]})") for c in out]

        return column_names

    @sync_action("table_preview")
    def table_preview(self):
        self._connection = self.init_connection()

        out = self._connection.execute(f"""
                SELECT *
                FROM delta_scan('{self.source_uri}')
                LIMIT 10;
                """).pl()

        formatted_output = self.to_markdown(out)

        return ValidationResult(formatted_output, MessageType.SUCCESS)

    def to_markdown(self, out):
        polars.Config.set_tbl_formatting("ASCII_MARKDOWN")
        polars.Config.set_tbl_hide_dataframe_shape(True)
        formatted_output = str(out)
        return formatted_output

    @sync_action("query_preview")
    def query_preview(self):
        self._connection = self.init_connection()

        query = self.params.data_selection.query.lower().replace(
            "from in_table", f"FROM delta_scan('{self.source_uri}')"
        )

        if "limit" not in query.lower():
            query = f"{query} LIMIT 10;"

        out = self._connection.execute(query).pl()

        formatted_output = self.to_markdown(out)

        return ValidationResult(formatted_output, MessageType.SUCCESS)

    @sync_action("list_uc_catalogs")
    def list_uc_catalogs(self):
        w = self._get_workspace_client()
        catalogs = w.catalogs.list()
        return [SelectElement(c.name) for c in catalogs]

    @sync_action("list_uc_schemas")
    def list_uc_schemas(self):
        w = self._get_workspace_client()
        schemas = w.schemas.list(self.params.source.catalog)
        return [SelectElement(s.name) for s in schemas]

    @sync_action("list_uc_tables")
    def list_uc_tables(self):
        w = self._get_workspace_client()
        tables = w.tables.list(self.params.source.catalog, self.params.source.schema_name)
        return [SelectElement(t.name) for t in tables]

    @sync_action("list_warehouses")
    def list_warehouses(self):
        w = self._get_workspace_client()
        return [SelectElement(wh.id, wh.name) for wh in w.warehouses.list()]

    @sync_action("workspace_query_preview")
    def workspace_query_preview(self):
        result_glob = self._execute_workspace_query(
            self.params.data_selection.query, row_limit=10, poll_timeout=PREVIEW_POLL_TIMEOUT
        )
        self._connection = self.init_connection()
        out = self._connection.execute(f"SELECT * FROM read_parquet('{result_glob}')").pl()
        return ValidationResult(self.to_markdown(out), MessageType.SUCCESS)


"""
        Main entrypoint
"""
if __name__ == "__main__":
    try:
        comp = Component()
        # this triggers the run method by default and is controlled by the configuration.action parameter
        comp.execute_action()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)
