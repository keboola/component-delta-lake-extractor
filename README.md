Delta Tables Extractor
=============

Component supports two access modes:

### 1. Direct Access to Delta Tables
Direct access to delta tables in your blob storage. We currently support the following providers:

- **AWS S3**: [Access Grants Credentials](https://docs.aws.amazon.com/AmazonS3/latest/userguide/access-grants-credentials.html)
- **Azure Blob Storage**: [Create SAS Tokens](https://learn.microsoft.com/en-us/azure/ai-services/translator/document-translation/how-to-guides/create-sas-tokens?tabs=Containers#create-sas-tokens-in-the-azure-portal)
- **Google Cloud Storage**: [Managing HMAC Keys](https://cloud.google.com/storage/docs/authentication/managing-hmackeys#console)

In this mode, the Delta Table path is defined by specifying the bucket/container and blob location where the table data is stored.

### 2. Unity Catalog
Currently we support only Azure Blob Storage backend.

**Setup Requirements:**
- **Access Token**: [How to get access token in Databricks](https://docs.databricks.com/aws/en/dev-tools/auth/pat#databricks-personal-access-tokens-for-workspace-users)
- **External Data Access**: [Enable external data access on the metastore](https://docs.databricks.com/aws/en/external-access/admin#enable-external-data-access-on-the-metastore)
- **Permissions**: Grant EXTERNAL USE SCHEMA permission
  - Navigate to: Workspace > Permissions > Add external use schema

In this mode, the user selects the catalog, schema, and table in the configuration row.


### Data selection options
The component supports the following data selection options:
- **All Data**: Select all columns and rows from the table.
- **Select Columns**: Select specific columns from the table.
- **Custom Query**: Write a custom SQL query to select specific data from the table.

### Data Destination Options
- **Store result as Parquet:** If enabled, the extractor will save the result as a Parquet file in file storage instead of a table in Keboola storage bucket.
- **Load Type:** In Full Load mode, the destination table is overwritten on each run. In Incremental Load mode, data is upserted into the destination table based on the primary key. Append mode does not use primary keys and does not deduplicate data.
- **Primary Key [optional]:** List of primary key columns for incremental loads. If not specified, incremental mode works as append mode.
- **Preserve Insertion Order:** Disabling this option may help prevent out-of-memory issues.
- **File Name:** Name of the output file. If left empty, a name is generated as bucket.table_name or catalog.schema.table_name when using Unity Catalog.
- **Table Name:** Name of the source table. If left empty, a name is generated as bucket.table_name or catalog.schema.table_name when using Unity Catalog.


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
