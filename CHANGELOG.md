# Changelog

## [CFTL-872] Azure storage endpoint on a non-default port (0.2.0)

### Added
- **Azure Storage Port** (`abs_port`, optional): the port of an Azure storage endpoint that is not
  reachable on 443 (for example a private endpoint or gateway). It applies to Azure Blob Storage in
  both access methods and is ignored for AWS S3 and Google Cloud Storage. With Unity Catalog, a port
  in the storage URL returned by Databricks takes precedence over this field.
- `unity_catalog_url` now documents that it accepts a port (`https://<workspace-host>:<port>`). This
  already worked before; only the description is new.
- **Debug logging** for the Azure storage path (`debug` parameter): the URL and credential type that
  Unity Catalog returned, the extracted account name, the `delta_scan` source URI and the explicit
  `BlobEndpoint`. The SAS token is never logged, only its length.
- **Azurite integration test** (`scripts/azurite_test.sh`, run in CI): a real `delta_scan` against
  Azurite over HTTPS on port 10010. It fails if a DuckDB or extension upgrade stops honouring the port.

### Fixed
- **Unity Catalog tables on an Azure storage endpoint with a non-default port.** The delta log was
  read, but the parquet files failed with `AzureBlobStorageFileSystem could not open file ... Could not
  connect to server`, because the `abfss://` (DFS) route drops the port and dials 443
  (duckdb/duckdb-azure#77). When a port is in effect, the component now reads the same data as
  `az://<container>/<path>` and pins `BlobEndpoint` (with the port) in the DuckDB Azure secret. Both
  readers of a `delta_scan` honour it: the azure extension reads `BlobEndpoint` directly, and
  duckdb-delta forwards it as the `azure_endpoint` of its Rust object_store.

### ⚠️ Backward compatibility
- Opt-in only. Without a port (field empty and no port in the Unity Catalog URL), the connection
  string and source URI are identical to 0.1.1, so existing configurations read exactly as before.
- A malformed Unity Catalog storage URL now fails with a user error instead of an internal error.

## [CFTL-719] Service principal auth + Databricks SQL query mode

### Added
- **Service principal (OAuth M2M) authentication** for Unity Catalog, as an alternative to the
  personal access token (PAT). PAT remains the default, so existing configurations are unaffected.
- **Databricks SQL query mode** (`workspace_query`): runs a custom SQL query on a Databricks SQL
  warehouse (selected per row) so multiple Unity Catalog tables can be joined. Results are streamed
  back via Cloud Fetch (`EXTERNAL_LINKS` + `ARROW_STREAM`).

### Fixed
- CSV output type mapping: parametrized/aliased DuckDB types are now handled — `DECIMAL(p,s)` and
  `FLOAT` columns were previously exported as `STRING`. Precision/scale (and varchar length) are now
  preserved (e.g. `DECIMAL(4,2)` → `NUMERIC(4,2)`).

### ⚠️ Backward compatibility
- The type-mapping fix changes decimal/float output columns from `STRING` to `NUMERIC`/`FLOAT` for
  **all** data-selection modes. An existing **incremental** configuration whose destination table was
  created with these columns as `STRING` may hit a type mismatch against the already-created Storage
  table and require a **one-time destination table reset** (full load / drop the existing table) after
  upgrading.
