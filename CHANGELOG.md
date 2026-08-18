# Changelog

## Azure storage port

### Added
- **Azure Storage Port** (`abs_port`, optional): allows specifying a non-standard port of the Azure
  storage endpoint (private endpoint, gateway or emulator). For direct storage access the port is
  passed as an explicit `BlobEndpoint` in the DuckDB Azure secret; for Unity Catalog access it is
  injected into the host of the `abfss://` URL returned by Databricks temporary table credentials,
  because DuckDB takes the endpoint from a fully qualified URL rather than from the secret.
  When the field is left empty nothing changes for existing configurations.
- **Debug logging** for the Azure storage path (enabled by the `debug` parameter): the URL and
  credential type returned by Unity Catalog temporary credentials, the storage account name extracted
  from that URL, the port substitution in the storage URL (including the reason when it is skipped),
  the resulting `delta_scan` source URI and the explicit `BlobEndpoint`. The SAS token itself is never
  logged - only its length.

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
