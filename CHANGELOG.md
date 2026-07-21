# Changelog

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
