# BigQuery → Semantic CSV, Cube YAMLs, and View YAML Generator

## Overview
This utility converts one or more Google BigQuery tables into:
- a consolidated semantic CSV (semantic_all.csv)
- individual Cube YAML files (one per table) under output/cubes
- an optional View YAML (e.g., deployments.yml) under output/views, built from the CSV’s join definitions

It supports iterative modeling:
1. Generate CSV + cube YAMLs from BigQuery schemas.
2. Manually enrich the CSV (add join paths, visibility, folders, etc.).
3. Build a view YAML using the enriched CSV.

Runs produce timestamped logs and separate error logs for traceability.

## Key Features
- Multiple table input in one command (append/update into a single CSV).
- Automatic dimension titles from column names (snake_case → Title Case).
- Automatic cube titles from cube names (snake_case → Title Case).
- Auto-detected primary key (Data Catalog tags → column descriptions → naming heuristics).
- Default measure: distinct count of the detected PK with generated title and description.
- Parallel Cube YAML generation (one YAML per cube).
- View generation driven by CSV after you add join definitions (first row per cube).
- Robust handling of multi-value joins (comma-separated or newline-separated lists).
- Detailed logs:
  - output/logs/log_YYYYMMDD_HHMMSS.md
  - output/logs/errors/errors_YYYYMMDD_HHMMSS.log

## Project Structure
- input/
  - semantic_all.csv (consolidated semantic schema)
- output/
  - cubes/
    - <cube_name>.yml
  - views/
    - <view_name>.yml
- logs/
  - log_YYYYMMDD_HHMMSS.md
  - errors/errors_YYYYMMDD_HHMMSS.log

## Prerequisites
- Python 3.9+ recommended
- Install packages:
```bash
pip install google-cloud-bigquery pandas
```
- Optional (for stronger PK detection via tags):
```bash
pip install google-cloud-datacatalog
```

## Authentication (Windows)
- CMD (current session):
```cmd
set GOOGLE_APPLICATION_CREDENTIALS=C:\path\to\service-account.json
```
- PowerShell (current session):
```powershell
$env:GOOGLE_APPLICATION_CREDENTIALS="C:\path\to\service-account.json"
```

The service account should have:
- BigQuery Data Viewer (to read table schemas)
- Data Catalog Viewer (optional, for PK detection via tags)

## Usage

### 1) Generate CSV + Cube YAMLs from BigQuery (no view yet)
- Multiple tables (comma-separated):
```bash
python utility.py --bq-tables project.dataset.table1,project.dataset.table2,project.dataset.table3 -i ./input --output-dir ./output --verbose
```
- Multiple tables (repeat flag):
```bash
python utility.py --bq-table project.dataset.table1 --bq-table project.dataset.table2 --bq-table project.dataset.table3 -i ./input --output-dir ./output --verbose
```

Outputs:
- input/semantic_all.csv: appended/updated rows for each cube
- output/cubes/<cube_name>.yml: one YAML per cube
- logs/log_YYYYMMDD_HHMMSS.md (+ errors if any)

### 2) Edit the CSV to add joins for view generation
Open input/semantic_all.csv and, for the first row of each cube, fill:
- join_primary_table: root cube name for that join row (often your main cube)
- join_secondary_table: list of related cubes (comma-separated or newline-separated)
- join_sql: list of corresponding join expressions (comma-separated or newline-separated)
- join_relationship: e.g., one_to_many or one_to_one
- Optionally set view_name, view_title, view_description, visible_in_view, view_folder_name

Example (first row for dim_product):
```text
cube_name,cube_sql_table,cube_description,cube_title,cube_data_source,view_name,view_title,view_description,visible_in_view,view_folder_name,join_primary_table,join_secondary_table,join_sql,join_relationship
dim_product,bcs-brd-data-innovation-np.dimensions.dim_product,Complete list of all the Head products,Dim Product,brd,,,,"",,dim_product,"dim_density
bei
h2h_tall_corn
hblup_h2h
writeups","{CUBE.key} = {dim_density.key}
{CUBE.key} = {bei.bei_product_key}
{CUBE.key} = {h2h_tall_corn.product_key}
{CUBE.key} = {hblup_h2h.agspp_product_key}
{CUBE.key} = {writeups.key}",one_to_many
```
Notes:
- The utility supports multiple values in “join_secondary_table” and “join_sql” as either newline-separated or comma-separated lists.
- “join_relationship” can be a single value; it will be reused for all listed joins if you don’t provide one per line.

### 3) Build the View YAML from the edited CSV
```bash
python utility.py --from-csv ./input/semantic_all.csv --output-dir ./output --view-name deployments --view-root-cube product --verbose
```

Outputs:
- output/views/deployments.yml
- logs/log_YYYYMMDD_HHMMSS.md (+ errors if any)

If you omit --view-root-cube, the utility auto-resolves the root by:
- Using your requested root if it exists
- Trying “dim_” prefixed variants
- Selecting the cube with the highest number of outgoing joins from the CSV
- Falling back to product/dim_product or the first cube alphabetically

## What gets auto-generated

### Dimensions
- Name = BigQuery column name
- Title = Titleized form of the column name (snake_case → Title Case)
- SQL = {CUBE}.<column>
- Type = Mapped from BigQuery type (STRING→string, INT64/FLOAT64/NUMERIC→number, BOOL→boolean, TIMESTAMP/DATE/DATETIME/TIME→time)

### Primary Key Detection
The utility infers PK via:
1. Data Catalog tags (primary_key / is_pk / pk true at column level)
2. Column description containing “primary key” or “pk”
3. Naming heuristics: <table>_id → first column ending with _id → id

### Default Measure
For the detected PK, adds:
- name: count_distinct_<pk>
- title: “Distinct count of <Titleized PK>”
- description: “This is to get Distinct count of <Titleized PK> recorded.”
- sql: “{<pk>}” (dimension reference style)
- type: count_distinct

Example:
```yaml
measures:
- name: count_distinct_capacity_request_id
  title: "Distinct count of Capacity Request ID"
  description: "This is to get Distinct count of Capacity Request ID recorded."
  sql: '{capacity_request_id}'
  type: count_distinct
```

## YAML Output Conventions
- Cube YAML file per cube: output/cubes/<cube_name>.yml
- View YAML built only in --from-csv mode: output/views/<view_name>.yml
- Strings in YAML are consistently quoted; SQL-like fields use single quotes.

## Example: Six-Table Run (Corn Deployments)
Generate CSV + cube YAMLs:
```bash
python utility.py --bq-tables bcs-brd-data-innovation-np.dimensions.dim_product,bcs-brd-data-innovation-np.dimensions.dim_density,bcs-brd-data-innovation-np.dimensions.dim_bei,bcs-brd-data-innovation-np.deployment.h2h_output_tall_corn_V3,bcs-brd-data-innovation-np.deployment.hblup_h2h,bcs-brd-data-innovation-np.deployment.writeups -i ./input --output-dir ./output --verbose
```
Edit input/semantic_all.csv to add joins (first row for each cube), then build the view:
```bash
python utility.py --from-csv ./input/semantic_all.csv --output-dir ./output --view-name deployments --view-root-cube product --verbose
```

## Troubleshooting
- Authentication error (DefaultCredentialsError):
  - Ensure GOOGLE_APPLICATION_CREDENTIALS is set to a valid service account JSON with BigQuery permissions.
- “Root cube not found for view generation.”
  - The utility now auto-resolves the root cube. If needed, pass --view-root-cube explicitly (e.g., product or dim_product).
- Missing cube joins in the view:
  - Confirm you added join_secondary_table and join_sql for the cube’s first row. Use newline-separated or comma-separated lists.
- Missing YAML files:
  - Cube YAMLs are only generated in BigQuery mode; view YAML is only generated in --from-csv mode.

## Notes
- You can re-run BigQuery mode to append/update the CSV and cube YAMLs as schemas evolve.
- You can re-run view mode whenever you refine joins in the CSV.
- Error logs are written to output/logs/errors/ with full stack traces to aid debugging.

Happy modeling!