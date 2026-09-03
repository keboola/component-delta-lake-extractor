Delta Tables Extractor
=============

Component supports two access modes:

### 1. Direct Access to Delta Tables
Direct access to delta tables in your blob storage. We currently support the following providers:

- **AWS S3**: [Access Grants Credentials](https://docs.aws.amazon.com/AmazonS3/latest/userguide/access-grants-credentials.html)
- **Azure Blob Storage**: [Create SAS Tokens](https://learn.microsoft.com/en-us/azure/ai-services/translator/document-translation/how-to-guides/create-sas-tokens?tabs=Containers#create-sas-tokens-in-the-azure-portal)
- **Google Cloud Storage**: [Managing HMAC Keys](https://cloud.google.com/storage/docs/authentication/managing-hmackeys#console)

In this mode, the Delta Table path is defined by specifying the bucket/container and blob location where the table data is stored.

**Azure Storage Port (optional):** if your storage endpoint is not reachable on the default HTTPS port (private endpoint, gateway or a storage emulator), fill in the **Azure Storage Port** field. It is passed as an explicit `BlobEndpoint` (`https://<account>.blob.core.windows.net:<port>`) in the storage credentials. Leave the field empty to use the default port. It applies to both access modes; in Unity Catalog mode a port stated in the storage URL returned by Databricks takes precedence, and this field covers the case where that URL carries no port.

Azure reads are always addressed over the blob endpoint (`az://<container>/<path>`), including in Unity Catalog mode where Databricks hands out an `abfss://` URL. The ADLS/DFS route cannot honour a non-default port: DuckDB's Azure extension passes the credentials straight to the Azure SDK for C++, which derives its DataLake endpoint from the account name unless the connection string carries a `DfsEndpoint` key, so the port is dropped and port 443 is dialled ([duckdb-azure#77](https://github.com/duckdb/duckdb-azure/issues/77)).

Set the `debug` parameter to `true` to log what Unity Catalog returned (storage URL, credential type), the source URI the read is addressed to, and the `BlobEndpoint` in use. The SAS token is never logged.

### 2. Unity Catalog
Currently we support only Azure Blob Storage backend.

**Authentication:** Two options are supported:
- **Personal Access Token (PAT)**: [How to get access token in Databricks](https://docs.databricks.com/aws/en/dev-tools/auth/pat#databricks-personal-access-tokens-for-workspace-users)
- **Service Principal (OAuth M2M)**: use a machine-to-machine identity instead of a personal token. Create it in *Settings > Identity and access > Service principals > Add service principal*, then generate an OAuth secret and use the resulting **Client ID** and **Client Secret**. When creating the secret, grant the **Databricks SQL access** and **Workspace access** scopes.

**Setup Requirements:**
- **External Data Access**: [Enable external data access on the metastore](https://docs.databricks.com/aws/en/external-access/admin#enable-external-data-access-on-the-metastore)
- **Permissions**: Grant EXTERNAL USE SCHEMA permission
  - Navigate to: Workspace > Permissions > Add external use schema
  - When using a service principal, also grant it `USE SCHEMA` and `SELECT` on the schema you want to read.

In this mode, the user selects the catalog, schema, and table in the configuration row.

**Databricks SQL query (joining multiple tables):** In addition to selecting a single table, Unity Catalog access supports a *Databricks SQL Query* data-selection mode. The SQL runs on a **Databricks SQL warehouse** (so it can join multiple tables by their full `catalog.schema.table` names), and the result is streamed back efficiently via Cloud Fetch. This requires:
- Selecting a **SQL Warehouse** per configuration row (in the row's data selection), so different queries can run on different warehouses (e.g. small vs large).
- Granting the principal (PAT user or service principal) **CAN USE** on the warehouse and `SELECT` on every table referenced in the query.


### Data selection options
The component supports the following data selection options:
- **All Data**: Select all columns and rows from the table.
- **Select Columns**: Select specific columns from the table.
- **Custom Query**: Write a custom SQL query, executed client-side in DuckDB over a single table. Reference the source table with the `in_table` placeholder.
- **Databricks SQL Query**: Write standard SQL executed on a Databricks SQL warehouse, referencing full `catalog.schema.table` names. Supports joining multiple tables (Unity Catalog access only).
### Data Destination Options
- **Store as Parquet:** If enabled, the extractor saves the result as a Parquet file in file storage instead of a table in the Keboola storage bucket.
- **Load Type:** In Full Load mode, the destination table is overwritten on each run. In Incremental Load mode, data is upserted into the destination table based on the primary key. Append mode does not use primary keys and does not deduplicate data.
- **Primary Key [optional]:** List of primary key columns for incremental loads. If not specified, incremental mode works as append mode.
- **Preserve Insertion Order:** Disabling this option may help prevent out-of-memory issues.
- **File Name:** Name of the output file (only when storing as Parquet). If left empty, a name is generated as `bucket.table_name` or `catalog.schema.table_name` when using Unity Catalog.
- **Table Name:** Name of the output table (only when storing in the storage bucket). If left empty, a name is generated as `bucket.table_name` or `catalog.schema.table_name` when using Unity Catalog.

Development
-----------

If required, change local data folder (the `CUSTOM_FOLDER` placeholder) path to
your custom path in the `docker-compose.yml` file:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    volumes:
      - ./:/code
      - ./CUSTOM_FOLDER:/data
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Clone this repository, init the workspace and run the component with following
command:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
git clone https://github.com/keboola/data-lake-tables data-lake-tables
cd data-lake-tables
docker-compose build
docker-compose run --rm dev
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Run the test suite and lint check using this command:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
docker-compose run --rm test
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Integration
===========

For information about deployment and integration with KBC, please refer to the
[deployment section of developers
documentation](https://developers.keboola.com/extend/component/deployment/)
