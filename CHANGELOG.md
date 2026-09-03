# Changelog

## [ST-4450] Azure storage port

### Added
- **Azure Storage Port** (`abs_port`, optional): allows specifying a non-standard port of the Azure
  storage endpoint (private endpoint, gateway or emulator) for **direct storage** access, where it is
  passed as an explicit `BlobEndpoint` in the DuckDB Azure secret. For **Unity Catalog** access the
  port is taken from the credentials URL issued by Databricks, so this field is not needed (and is
  ignored). When the field is left empty nothing changes for existing configurations.
- **Debug logging** for the Azure storage path (enabled by the `debug` parameter): the URL and
  credential type returned by Unity Catalog temporary credentials, the storage account name extracted
  from that URL, the resulting `delta_scan` source URI and the explicit `BlobEndpoint`. The SAS token
  itself is never logged - only its length.

### Fixed
- **Reading a Unity Catalog table whose Azure storage endpoint uses a non-default port.** The delta log
  was read successfully, but the parquet data files failed with `AzureBlobStorageFileSystem could not
  open file ... Could not connect to server` — the port had been dropped and port 443 was dialled.

  Unity Catalog hands out an `abfss://<container>@<account>.dfs.<suffix>[:<port>]/<path>` URL, and that
  DFS route cannot honour the port: the Azure SDK for C++ builds its DataLake service URL from the
  connection string's `DfsEndpoint` key alone and otherwise rebuilds it from `AccountName`, so neither
  the port in the URL nor a `BlobEndpoint` pinning host:port reaches the reader
  (duckdb/duckdb-azure#77). The component now addresses the same data over the blob endpoint as
  `az://<container>/<path>` and pins `BlobEndpoint` (port included) in the secret — the combination
  duckdb-azure and duckdb-delta test against Azurite, which itself runs on a non-default port. Both
  readers of a `delta_scan` then honour it: the C++ azure extension takes the endpoint straight from
  `BlobEndpoint`, and delta's Rust object_store gets it because duckdb-delta forwards `BlobEndpoint`
  on as its `azure_endpoint` option.

### ⚠️ Backward compatibility
- Unity Catalog accounts on the default storage endpoint are unaffected: the emitted `BlobEndpoint` is
  then byte-for-byte what the Azure SDK would have derived from `AccountName` on its own.

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
