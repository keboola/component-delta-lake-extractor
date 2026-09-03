import unittest
import mock
import os
from types import SimpleNamespace as NS

from freezegun import freeze_time

import pyarrow as pa
from databricks.sdk.service.sql import StatementState
from keboola.component.dao import SupportedDataTypes
from keboola.component.exceptions import UserException

from component import Component
from configuration import Configuration


def make_component(**param_overrides):
    """Build a Component instance without running ComponentBase.__init__ (no datadir needed)."""
    params = {
        "access_method": "unity_catalog",
        "unity_catalog_url": "https://workspace.example.com",
        "source": {},
        "data_selection": {},
        "destination": {},
    }
    params.update(param_overrides)
    comp = Component.__new__(Component)
    comp.params = Configuration(**params)
    comp._workspace_result_glob = None
    comp.source_uri = None
    return comp


def fake_response(state, external_links=None, next_chunk_index=None, columns=None, error=None):
    status = NS(state=state, error=(NS(message=error) if error else None))
    result = None
    if external_links is not None:
        result = NS(external_links=external_links, next_chunk_index=next_chunk_index)
    manifest = NS(schema=NS(columns=columns or []))
    return NS(statement_id="stmt-1", status=status, result=result, manifest=manifest)


def fake_link(url="https://presigned/chunk", headers=None):
    return NS(external_link=url, http_headers=headers)


def fake_column(name, type_name, precision=None, scale=None):
    return NS(name=name, type_name=NS(value=type_name), type_precision=precision, type_scale=scale)


class TestComponent(unittest.TestCase):
    # set global time to 2010-10-10 - affects functions like datetime.now()
    @freeze_time("2010-10-10")
    # set KBC_DATADIR env to non-existing dir
    @mock.patch.dict(os.environ, {"KBC_DATADIR": "./non-existing-dir"})
    def test_run_no_cfg_fails(self):
        with self.assertRaises(ValueError):
            comp = Component()
            comp.run()

    # --- type mapping -----------------------------------------------------------------

    def test_convert_base_types(self):
        cases = {
            "DECIMAL(10,0)": SupportedDataTypes.NUMERIC,
            "DECIMAL(18,2)": SupportedDataTypes.NUMERIC,
            "NUMERIC": SupportedDataTypes.NUMERIC,
            "FLOAT": SupportedDataTypes.FLOAT,
            "REAL": SupportedDataTypes.FLOAT,
            "DOUBLE": SupportedDataTypes.FLOAT,
            "BIGINT": SupportedDataTypes.INTEGER,
            "BOOLEAN": SupportedDataTypes.BOOLEAN,
            "TIMESTAMP WITH TIME ZONE": SupportedDataTypes.TIMESTAMP,
            "DATE": SupportedDataTypes.DATE,
            "VARCHAR": SupportedDataTypes.STRING,
            # nested / complex types have no scalar equivalent -> STRING
            "INTEGER[]": SupportedDataTypes.STRING,
            "DECIMAL(10,2)[]": SupportedDataTypes.STRING,
            "STRUCT(a INTEGER, b VARCHAR)": SupportedDataTypes.STRING,
            "MAP(VARCHAR, INTEGER)": SupportedDataTypes.STRING,
        }
        for dtype, expected in cases.items():
            self.assertEqual(Component.convert_base_types(dtype), expected, msg=dtype)

    def test_to_base_type_preserves_length(self):
        cases = {
            "DECIMAL(4,2)": (SupportedDataTypes.NUMERIC, "4,2"),
            "DECIMAL(38, 18)": (SupportedDataTypes.NUMERIC, "38,18"),
            "VARCHAR(255)": (SupportedDataTypes.STRING, "255"),
            "BIGINT": (SupportedDataTypes.INTEGER, None),
            "DOUBLE": (SupportedDataTypes.FLOAT, None),
            "DATE": (SupportedDataTypes.DATE, None),
            # complex types -> STRING with NO length (no garbage from the inner parens)
            "DECIMAL(10,2)[]": (SupportedDataTypes.STRING, None),
            "INTEGER[]": (SupportedDataTypes.STRING, None),
            "STRUCT(a INTEGER, b VARCHAR)": (SupportedDataTypes.STRING, None),
            "MAP(VARCHAR, INTEGER)": (SupportedDataTypes.STRING, None),
        }
        for dtype, (expected_base, expected_len) in cases.items():
            bt = Component.to_base_type(dtype)["base"]
            self.assertEqual(bt.dtype, expected_base.value, msg=dtype)
            self.assertEqual(bt.length, expected_len, msg=dtype)

    # --- auth selection ---------------------------------------------------------------

    @mock.patch("component.WorkspaceClient")
    def test_get_workspace_client_pat(self, wc):
        comp = make_component(auth_type="pat", **{"#unity_catalog_token": "dapi-token"})
        comp._get_workspace_client()
        wc.assert_called_once_with(host="https://workspace.example.com", token="dapi-token")

    @mock.patch("component.WorkspaceClient")
    def test_get_workspace_client_service_principal(self, wc):
        comp = make_component(
            auth_type="service_principal",
            unity_catalog_client_id="client-id",
            **{"#unity_catalog_client_secret": "client-secret"},
        )
        comp._get_workspace_client()
        wc.assert_called_once_with(
            host="https://workspace.example.com",
            client_id="client-id",
            client_secret="client-secret",
        )

    # --- query building & naming ------------------------------------------------------

    def test_get_query_workspace_query(self):
        comp = make_component(data_selection={"mode": "workspace_query", "query": "SELECT 1"})
        comp._workspace_result_glob = "/tmp/duckdb/dbx_result/*.parquet"
        self.assertEqual(
            comp.get_query(),
            "SELECT * FROM read_parquet('/tmp/duckdb/dbx_result/*.parquet')",
        )

    def test_get_query_all_data(self):
        comp = make_component(data_selection={"mode": "all_data"})
        comp.source_uri = "az://container/blob"
        self.assertEqual(comp.get_query(), "SELECT * FROM delta_scan('az://container/blob')")

    def test_get_table_name_fallback_for_workspace_query(self):
        comp = make_component(data_selection={"mode": "workspace_query", "query": "SELECT 1"})
        self.assertEqual(comp.get_table_name(), "query_result")

    def test_get_table_name_uses_destination(self):
        comp = make_component(destination={"table_name": "my_out"})
        self.assertEqual(comp.get_table_name(), "my_out")

    # --- workspace query execution ----------------------------------------------------

    def test_execute_workspace_query_requires_warehouse(self):
        comp = make_component(data_selection={"mode": "workspace_query", "query": "SELECT 1"})
        with self.assertRaises(UserException):
            comp._execute_workspace_query("SELECT 1")

    def test_execute_workspace_query_requires_query(self):
        comp = make_component(
            data_selection={"mode": "workspace_query", "query": "SELECT 1", "warehouse_id": "wh1"}
        )
        with self.assertRaises(UserException):
            comp._execute_workspace_query("")

    @mock.patch("component.pq")
    @mock.patch("component.pa")
    @mock.patch("component.requests")
    @mock.patch("component.os.makedirs")
    def test_execute_workspace_query_success_multichunk(self, _md, req, _pa, pq):
        comp = make_component(
            data_selection={"mode": "workspace_query", "query": "SELECT 1", "warehouse_id": "wh1"}
        )
        req.get.return_value.content = b""
        w = mock.MagicMock()
        comp._get_workspace_client = lambda: w
        # first chunk points to a second chunk, second one ends the paging
        w.statement_execution.execute_statement.return_value = fake_response(
            StatementState.SUCCEEDED, external_links=[fake_link()], next_chunk_index=1
        )
        w.statement_execution.get_statement_result_chunk_n.return_value = NS(
            external_links=[fake_link(url="https://presigned/chunk2")], next_chunk_index=None
        )

        glob = comp._execute_workspace_query("SELECT 1")

        self.assertTrue(glob.endswith(os.path.join("dbx_result", "*.parquet")))
        self.assertEqual(req.get.call_count, 2)
        self.assertEqual(pq.write_table.call_count, 2)
        w.statement_execution.get_statement_result_chunk_n.assert_called_once_with("stmt-1", 1)
        # presigned download must carry an explicit timeout
        self.assertEqual(req.get.call_args.kwargs.get("timeout"), (10, 300))

    def test_execute_workspace_query_download_error_raises_userexception(self):
        import requests

        comp = make_component(
            data_selection={"mode": "workspace_query", "query": "SELECT 1", "warehouse_id": "wh1"}
        )
        w = mock.MagicMock()
        comp._get_workspace_client = lambda: w
        w.statement_execution.execute_statement.return_value = fake_response(
            StatementState.SUCCEEDED, external_links=[fake_link()], next_chunk_index=None
        )
        # patch only requests.get so the real requests.RequestException hierarchy stays intact
        with mock.patch("component.requests.get", side_effect=requests.ConnectionError("boom")):
            with self.assertRaises(UserException) as ctx:
                comp._execute_workspace_query("SELECT 1")
        msg = str(ctx.exception)
        self.assertIn("Failed to download query result", msg)
        self.assertIn("ConnectionError", msg)
        self.assertNotIn("boom", msg)  # raw exception text must not leak

    def test_execute_workspace_query_download_error_does_not_leak_sas_url(self):
        import requests

        comp = make_component(
            data_selection={"mode": "workspace_query", "query": "SELECT 1", "warehouse_id": "wh1"}
        )
        w = mock.MagicMock()
        comp._get_workspace_client = lambda: w
        secret_url = "https://acct.blob.core.windows.net/x?sig=SECRETSAS&se=2026"
        w.statement_execution.execute_statement.return_value = fake_response(
            StatementState.SUCCEEDED, external_links=[fake_link(url=secret_url)], next_chunk_index=None
        )
        http_err = requests.HTTPError(
            f"403 Client Error: Forbidden for url: {secret_url}", response=mock.MagicMock(status_code=403)
        )
        with mock.patch("component.requests.get", side_effect=http_err):
            with self.assertRaises(UserException) as ctx:
                comp._execute_workspace_query("SELECT 1")
        msg = str(ctx.exception)
        self.assertNotIn("SECRETSAS", msg)
        self.assertNotIn("sig=", msg)
        self.assertIn("HTTP 403", msg)

    @mock.patch("component.time.sleep")
    def test_execute_workspace_query_poll_timeout_cancels(self, _sleep):
        comp = make_component(
            data_selection={"mode": "workspace_query", "query": "SELECT 1", "warehouse_id": "wh1"}
        )
        w = mock.MagicMock()
        comp._get_workspace_client = lambda: w
        w.statement_execution.execute_statement.return_value = fake_response(StatementState.RUNNING)
        w.statement_execution.get_statement.return_value = fake_response(StatementState.RUNNING)
        with self.assertRaises(UserException) as ctx:
            comp._execute_workspace_query("SELECT 1", poll_timeout=0)
        w.statement_execution.cancel_execution.assert_called_once_with("stmt-1")
        self.assertIn("cancelled", str(ctx.exception))

    @mock.patch("component.pq")
    @mock.patch("component.pa")
    @mock.patch("component.requests")
    @mock.patch("component.time.sleep")
    @mock.patch("component.os.makedirs")
    def test_execute_workspace_query_polls_until_succeeded(self, _md, sleep, req, _pa, pq):
        comp = make_component(
            data_selection={"mode": "workspace_query", "query": "SELECT 1", "warehouse_id": "wh1"}
        )
        req.get.return_value.content = b""
        w = mock.MagicMock()
        comp._get_workspace_client = lambda: w
        w.statement_execution.execute_statement.return_value = fake_response(StatementState.PENDING)
        w.statement_execution.get_statement.side_effect = [
            fake_response(StatementState.RUNNING),
            fake_response(StatementState.SUCCEEDED, external_links=[fake_link()], next_chunk_index=None),
        ]

        comp._execute_workspace_query("SELECT 1")

        self.assertEqual(w.statement_execution.get_statement.call_count, 2)
        self.assertEqual(pq.write_table.call_count, 1)
        self.assertTrue(sleep.called)

    @mock.patch("component.os.makedirs")
    def test_execute_workspace_query_failed_state_raises(self, _md):
        comp = make_component(
            data_selection={"mode": "workspace_query", "query": "SELECT 1", "warehouse_id": "wh1"}
        )
        w = mock.MagicMock()
        comp._get_workspace_client = lambda: w
        w.statement_execution.execute_statement.return_value = fake_response(
            StatementState.FAILED, error="table not found"
        )
        with self.assertRaises(UserException) as ctx:
            comp._execute_workspace_query("SELECT 1")
        self.assertIn("table not found", str(ctx.exception))

    @mock.patch("component.pq")
    @mock.patch("component.pa")
    @mock.patch("component.requests")
    @mock.patch("component.os.makedirs")
    def test_execute_workspace_query_empty_result_writes_empty_parquet(self, _md, req, _pa, pq):
        comp = make_component(
            data_selection={"mode": "workspace_query", "query": "SELECT 1", "warehouse_id": "wh1"}
        )
        w = mock.MagicMock()
        comp._get_workspace_client = lambda: w
        w.statement_execution.execute_statement.return_value = fake_response(
            StatementState.SUCCEEDED,
            external_links=[],
            next_chunk_index=None,
            columns=[fake_column("a", "STRING"), fake_column("b", "LONG")],
        )

        comp._execute_workspace_query("SELECT 1")

        # no chunks downloaded, but one empty parquet written from the manifest schema
        req.get.assert_not_called()
        self.assertEqual(pq.write_table.call_count, 1)

    def test_arrow_type_from_column(self):
        cases = {
            ("STRING", None, None): pa.string(),
            ("LONG", None, None): pa.int64(),
            ("INT", None, None): pa.int32(),
            ("DOUBLE", None, None): pa.float64(),
            ("FLOAT", None, None): pa.float32(),
            ("BOOLEAN", None, None): pa.bool_(),
            ("DATE", None, None): pa.date32(),
            ("TIMESTAMP", None, None): pa.timestamp("us"),
            ("DECIMAL", 10, 2): pa.decimal128(10, 2),
            ("ARRAY", None, None): pa.string(),  # unmapped -> string
        }
        for (type_name, precision, scale), expected in cases.items():
            col = fake_column("c", type_name, precision, scale)
            self.assertEqual(Component._arrow_type_from_column(col), expected, msg=type_name)

    # --- azure storage port -----------------------------------------------------------

    def test_abs_connection_query_without_port_has_no_endpoint(self):
        comp = make_component(
            access_method="direct_storage",
            provider="abs",
            abs_account_name="acct",
            **{"#abs_sas_token": "sv=2024"},
        )
        query = comp.build_connection_query()
        self.assertIn("AccountName=acct;SharedAccessSignature=sv=2024", query)
        self.assertNotIn("BlobEndpoint", query)

    def test_abs_connection_query_with_port_sets_blob_endpoint(self):
        comp = make_component(
            access_method="direct_storage",
            provider="abs",
            abs_account_name="acct",
            abs_port=10000,
            **{"#abs_sas_token": "sv=2024"},
        )
        query = comp.build_connection_query()
        self.assertIn("BlobEndpoint=https://acct.blob.core.windows.net:10000", query)

    def test_abfss_to_blob_keeps_port(self):
        self.assertEqual(
            Component._abfss_to_blob("abfss://cont@acct.dfs.core.windows.net:10000/schema/table"),
            ("acct", "https://acct.blob.core.windows.net:10000", "az://cont/schema/table"),
        )

    def test_abfss_to_blob_without_port(self):
        self.assertEqual(
            Component._abfss_to_blob("abfss://cont@acct.dfs.core.windows.net/schema/table"),
            ("acct", "https://acct.blob.core.windows.net", "az://cont/schema/table"),
        )

    def test_abfss_to_blob_rejects_unexpected_url(self):
        for url in ("abfss://acct.dfs.core.windows.net/table", "https://acct/table"):
            with self.assertRaises(UserException, msg=url):
                Component._abfss_to_blob(url)

    def test_abfss_to_blob_rejects_invalid_port(self):
        with self.assertRaises(UserException):
            Component._abfss_to_blob("abfss://cont@acct.dfs.core.windows.net:nope/table")

    def test_abfss_to_blob_falls_back_to_configured_port(self):
        # A private-endpoint workspace can return a portless URL even when storage is not on 443.
        self.assertEqual(
            Component._abfss_to_blob("abfss://cont@acct.dfs.core.windows.net/schema/table", 44317),
            ("acct", "https://acct.blob.core.windows.net:44317", "az://cont/schema/table"),
        )

    def test_abfss_to_blob_url_port_wins_over_fallback(self):
        self.assertEqual(
            Component._abfss_to_blob("abfss://cont@acct.dfs.core.windows.net:8443/schema/table", 44317)[1],
            "https://acct.blob.core.windows.net:8443",
        )

    def test_unity_catalog_uses_blob_endpoint_with_port_from_url(self):
        comp = make_component()
        comp._get_workspace_client = lambda: mock.MagicMock()
        comp._get_temp_credentials = lambda w: NS(
            url="abfss://cont@acct.dfs.core.windows.net:10000/schema/table",
            aws_temp_credentials=None,
            azure_user_delegation_sas=NS(sas_token="sv=2024"),
        )

        query = comp.build_connection_query()

        # The abfss/DFS route cannot honour the port, so the data is addressed over blob instead.
        self.assertEqual(comp.source_uri, "az://cont/schema/table")
        self.assertIn("AccountName=acct;", query)
        self.assertIn("BlobEndpoint=https://acct.blob.core.windows.net:10000", query)

    def test_unity_catalog_without_port_pins_default_blob_endpoint(self):
        comp = make_component()
        comp._get_workspace_client = lambda: mock.MagicMock()
        comp._get_temp_credentials = lambda w: NS(
            url="abfss://cont@acct.dfs.core.windows.net/schema/table",
            aws_temp_credentials=None,
            azure_user_delegation_sas=NS(sas_token="sv=2024"),
        )

        query = comp.build_connection_query()

        self.assertEqual(comp.source_uri, "az://cont/schema/table")
        # Identical to what the Azure SDK derives from AccountName, so no behaviour change.
        self.assertIn("BlobEndpoint=https://acct.blob.core.windows.net'", query)

    def test_unity_catalog_portless_url_uses_configured_port(self):
        # ST-4450: the workspace returns a portless URL, so abs_port is the only source of the port.
        comp = make_component(abs_port=44317)
        comp._get_workspace_client = lambda: mock.MagicMock()
        comp._get_temp_credentials = lambda w: NS(
            url="abfss://cont@acct.dfs.core.windows.net/schema/table",
            aws_temp_credentials=None,
            azure_user_delegation_sas=NS(sas_token="sv=2024"),
        )

        query = comp.build_connection_query()

        self.assertEqual(comp.source_uri, "az://cont/schema/table")
        self.assertIn("BlobEndpoint=https://acct.blob.core.windows.net:44317", query)

    def test_unity_catalog_prefers_url_port_over_configured_port(self):
        comp = make_component(abs_port=10000)
        comp._get_workspace_client = lambda: mock.MagicMock()
        comp._get_temp_credentials = lambda w: NS(
            url="abfss://cont@acct.dfs.core.windows.net:8443/schema/table",
            aws_temp_credentials=None,
            azure_user_delegation_sas=NS(sas_token="sv=2024"),
        )

        query = comp.build_connection_query()

        self.assertIn("BlobEndpoint=https://acct.blob.core.windows.net:8443", query)
        self.assertNotIn("10000", query)

    def test_direct_storage_abs_source_uri_ignores_port(self):
        comp = make_component(
            access_method="direct_storage",
            provider="abs",
            abs_account_name="acct",
            abs_port=10000,
            source={"container_name": "cont", "blob_name": "tbl"},
        )
        self.assertEqual(comp.build_source_uri(), "az://cont/tbl")

    def test_abs_port_empty_string_is_none(self):
        comp = make_component(abs_port="")
        self.assertIsNone(comp.params.abs_port)

    def test_abs_port_out_of_range_raises_user_exception(self):
        with self.assertRaises(UserException):
            make_component(abs_port=99999)

    # --- debug logging ----------------------------------------------------------------

    def test_unity_catalog_debug_log_reports_url_and_target(self):
        comp = make_component()
        comp._get_workspace_client = lambda: mock.MagicMock()
        comp._get_temp_credentials = lambda w: NS(
            url="abfss://cont@acct.dfs.core.windows.net:10000/schema/table",
            aws_temp_credentials=None,
            azure_user_delegation_sas=NS(sas_token="sv=2024&sig=SECRETSAS"),
            expiration_time=1700000000,
        )

        with self.assertLogs(level="DEBUG") as logs:
            comp.build_connection_query()
        output = "\n".join(logs.output)

        # what Unity Catalog returned
        self.assertIn("abfss://cont@acct.dfs.core.windows.net:10000/schema/table", output)
        self.assertIn("azure_user_delegation_sas", output)
        self.assertIn("acct", output)
        # where the read is actually addressed
        self.assertIn("az://cont/schema/table", output)
        self.assertIn("BlobEndpoint 'https://acct.blob.core.windows.net:10000'", output)
        # the SAS token itself must never be logged
        self.assertNotIn("SECRETSAS", output)
        self.assertNotIn("sig=", output)

    def test_unity_catalog_debug_log_notes_overridden_port(self):
        comp = make_component(abs_port=10000)
        comp._get_workspace_client = lambda: mock.MagicMock()
        comp._get_temp_credentials = lambda w: NS(
            url="abfss://cont@acct.dfs.core.windows.net:8443/schema/table",
            aws_temp_credentials=None,
            azure_user_delegation_sas=NS(sas_token="sv=2024"),
        )

        with self.assertLogs(level="DEBUG") as logs:
            comp.build_connection_query()

        self.assertIn("overridden", "\n".join(logs.output).lower())

    # --- sync action ------------------------------------------------------------------

    def test_list_warehouses(self):
        comp = make_component()
        w = mock.MagicMock()
        w.warehouses.list.return_value = [NS(id="wh1", name="Small"), NS(id="wh2", name="Large")]
        comp._get_workspace_client = lambda: w
        # call the undecorated method (the @sync_action wrapper needs ComponentBase internals)
        result = Component.list_warehouses.__wrapped__(comp)
        self.assertEqual([(e.value, e.label) for e in result], [("wh1", "Small"), ("wh2", "Large")])


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
