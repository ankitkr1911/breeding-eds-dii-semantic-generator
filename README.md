# BigQuery → Semantic CSV/YAML Utility

A Python CLI to generate and maintain semantic modeling artifacts from Google BigQuery tables:
- Auto-build a CSV skeleton with dimensions and a default measure.
- Render YAML files (one per table/cube) in a consistent format.
- Support multiple tables per run, idempotent upsert into a single CSV.
- Append a run log and capture errors separately for traceability.

## Features

- Automatic CSV generation for one or more BigQuery tables:
  - One row per column as a dimension.
  - Auto-derived dimension_title from column name (e.g., capacity_request_id → “Capacity Request ID”).
  - Auto-derived cube_title from cube name (e.g., capacity_request → “Capacity Request”).
  - cube_data_source set to the second dash-separated part of the project ID (e.g., bcs-breeding-datasets → “breeding”).
- Default measure (if PK detected):
  - name: count_distinct_<pk>
  - title: “Distinct count of <PK Title>”
  - description: “This is to get Distinct count of <PK Title> recorded in the <dataset> application”
  - sql: '{<pk>}'
  - type: count_distinct
- YAML generation per cube, in parallel:
  - Output files named <cube>.yml (e.g., capacity_request.yml).
- CSV edit-and-rebuild mode:
  - Edit the generated CSV; rebuild YAMLs from it.
- Logging and error handling:
  - Markdown run log embedding all YAMLs for the run.
  - Separate error log with stack traces.

## Project Structure

```
.
├─ input/
│  └─ semantic_all.csv          # Combined CSV; upserted per run
├─ output/
│  ├─ capacity_request.yml      # YAML per cube (table)
│  ├─ <other_cube>.yml
│  └─ logs/
│     ├─ log_YYYYMMDD_HHMMSS.md
│     └─ errors/
│        └─ errors_YYYYMMDD_HHMMSS.log
└─ utility.py                    # CLI entry point
```

## Prerequisites

- Python 3.9+ (tested on Python 3.10)
- Packages:
  - pandas
  - google-cloud-bigquery
  - google-cloud-datacatalog (optional; used for PK detection via tags)

Install packages:
```bash
pip install pandas google-cloud-bigquery google-cloud-datacatalog
```

### Authenticate to Google Cloud

BigQuery client needs credentials (ADC).

Option A: Cloud SDK (user credentials)
- Install SDK (Windows):
  - via winget: winget install -e --id Google.CloudSDK
  - or via installer: https://cloud.google.com/sdk/docs/install
- Then:
```bash
gcloud auth application-default login
gcloud config set project <project-id>
```

Option B: Service Account key (no SDK needed)
- Create a service account with roles:
  - BigQuery Data Viewer (required)
  - Data Catalog Viewer (optional; for PK detection via tags)
- Download JSON key and set env var:
  - CMD (current session): set GOOGLE_APPLICATION_CREDENTIALS=C:\path\to\key.json
  - PowerShell: $env:GOOGLE_APPLICATION_CREDENTIALS="C:\path\to\key.json"

## Usage

### Generate from a single BigQuery table

- Appends/updates input/semantic_all.csv
- Writes YAML to output/<cube>.yml
- Creates run log and error log (if any)

```bash
python utility.py --bq-table bcs-breeding-datasets.velocity.capacity_request -i ./input --output-dir ./output --verbose
```

### Generate from multiple tables

Pass multiple flags:
```bash
python utility.py --bq-table proj.ds.table1 --bq-table proj.ds.table2 -i ./input --output-dir ./output --verbose
```

Or comma-separated:
```bash
python utility.py --bq-tables proj.ds.table1,proj.ds.table2 -i ./input --output-dir ./output --verbose
```

### Rebuild YAMLs from edited CSV

After editing input/semantic_all.csv (e.g., adding descriptions, joins):
```bash
python utility.py --from-csv ./input/semantic_all.csv --output-dir ./output --verbose
```

## CSV Schema (columns)

- dimension_name
- dimension_measure_flag (“dimension” | “measure”)
- dimension_title
- dimension_description
- dimension_sql
- primary_key (“TRUE” for PK dimension)
- dimension_type (e.g., string, number, time; for measure: count_distinct)
- cube_name
- cube_sql_table (project.dataset.table)
- cube_description
- cube_title
- cube_data_source
- view_name, view_title, view_description, visible_in_view, view_folder_name
- join_primary_table, join_secondary_table, join_sql, join_relationship

Notes:
- dimension_title is auto derived from dimension_name.
- cube_title is auto derived from cube_name.
- cube_data_source is derived from the project ID (second dash-separated token).

## YAML Structure

Each cube renders as:
```yaml
cubes:
- description: "..."
  name: capacity_request
  sql_table: 'bcs-breeding-datasets.velocity.capacity_request'
  title: "Capacity Request"

  joins:
  # Optional; only if present in CSV (manually added)
  - name: experiment_sets_entries
    relationship: one_to_many
    sql: '{CUBE.capacity_request_id} = {experiment_sets_entries.capacity_request_id}'

  dimensions:
  #----------joining keys--------------
  - name: capacity_request_id
    title: "Capacity Request ID"
    description: "..."
    sql: '{CUBE}.capacity_request_id'
    primaryKey: true
    type: number

  measures:
  - name: count_distinct_capacity_request_id
    title: "Distinct count of Capacity Request ID"
    description: "This is to get Distinct count of Capacity Request ID recorded in the velocity application"
    sql: '{capacity_request_id}'
    type: count_distinct
```

Filenames:
- output/<cube>.yml (e.g., output/capacity_request.yml)

## Primary Key Detection

BigQuery does not enforce PKs. The utility infers PK using:
1. Data Catalog tags (if available):
   - Tag fields named primary_key, is_pk, or pk with true-ish values, attached to columns.
2. BigQuery column descriptions:
   - Contains “primary key” or “pk”.
3. Naming heuristics:
   - <table>_id → any column ending with _id → id.

If a PK is detected, the default measure is added. If not, no default measure is created; you can set the primary_key flag in CSV manually and rebuild YAML.

## Logging

- Run log (Markdown) embedding YAML outputs:
  - output/logs/log_YYYYMMDD_HHMMSS.md
- Error log:
  - output/logs/errors/errors_YYYYMMDD_HHMMSS.log

## Troubleshooting

- SyntaxError at line 1:
  - Cause: Code fences (```python) pasted into utility.py.
  - Fix: Remove backticks; utility.py must be pure Python.

- Default credentials not found:
  - Cause: ADC not set.
  - Fix: Use Cloud SDK (gcloud auth application-default login) or set GOOGLE_APPLICATION_CREDENTIALS to a service account JSON key.

- “unrecognized arguments: -o …”:
  - Cause: Updated CLI uses --output-dir, not -o.
  - Fix: Use --output-dir ./output.

- Duplicate rows in CSV:
  - Handled by upsert: the utility removes existing rows for a cube and writes fresh ones.

## Development Notes

- Rendering is manual to match the existing YAML style and quoting:
  - Double quotes for display strings; single quotes for SQL-like fragments.
- Data Catalog dependency is optional and handled gracefully; if unavailable, the utility falls back to descriptions and heuristics.
- The utility supports both generation from BigQuery and rebuild from CSV to accommodate manual edits.

## License

Internal utility for semantic modeling. Adjust, extend, or integrate as needed within your environment.