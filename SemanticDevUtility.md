# Semantic Development Utility

## Overview
This utility turns one or more Google BigQuery tables into:
- individual Cube YAML files (one per table) under output/cubes
- a consolidated semantic CSV (semantic_all.csv)
- a View YAML under output/views, built from the CSV’s join definitions and view metadata

It supports an iterative workflow:
1. Generate CSV + cube YAMLs from BigQuery (read-only).
2. Edit the CSV (add joins, descriptions, folders, view metadata).
3. Build the view from the edited CSV and update cubes accordingly.

All runs produce timestamped logs and separate error logs.

## Folder structure
- input/
  - semantic_all.csv
- output/
  - cubes/
    - <cube_name>.yml
  - views/
    - <view_name>.yml
- logs/
  - log_YYYYMMDD_HHMMSS.md
  - errors/errors_YYYYMMDD_HHMMSS.log
- utility.py

## Prerequisites
- Python 3.9+ recommended
- Packages:
  - pandas
  - google-cloud-bigquery
  - google-cloud-datacatalog (optional; improves PK detection when Data Catalog tags exist)

## Setup (macOS example)
Use this section as-is or adapt paths for your machine.

1) Set credentials (run once per terminal session)
```bash
export GOOGLE_APPLICATION_CREDENTIALS="BigQuery/credentials.json file path on your system"
```

2) Change to your project directory (run once per terminal session)
Replace with your local path to the utility project.
```bash
cd "/utilityUS/Semantic_Development_Utility file path on your system"
```

3) Set up Python environment (one-time per machine or when missing packages)
- Option A: Use a virtual environment (recommended)
```bash
python3 -m venv .venv
source .venv/bin/activate

python -m pip install --upgrade pip
python -m pip install pandas google-cloud-bigquery google-cloud-datacatalog
```
- Option B: Install packages for your user (if not using venv)
```bash
python3 -m pip install --user --upgrade pip
python3 -m pip install --user pandas google-cloud-bigquery google-cloud-datacatalog
```

## Workflow

Step 4) Generate CSV and cube YAMLs from BigQuery tables (repeat this whenever you add/update tables; this step does not generate the view)
- Comma-separated list:
```bash
python3 utility.py --bq-tables <project.dataset.table1>,<project.dataset.table2>,<project.dataset.table3> -i ./input --output-dir ./output --verbose
```

Outputs after Step 4:
- CSV: ./input/semantic_all.csv (rows appended/updated per table)
- Cube YAMLs: ./output/cubes/<cube_name>.yml
- Logs: ./output/logs/log_YYYYMMDD_HHMMSS.md (errors in ./output/logs/errors/)

Step 5) Edit the CSV to add joins and metadata (repeat whenever you want to change relationships, descriptions, folders, or view content)
Open ./input/semantic_all.csv and, for the first row of each cube you want in the view, fill:
- join_primary_table: your “main” cube name for that cube’s join row (e.g., influx_field_cassette_cell)
- join_secondary_table: list of related cubes (newline-separated or comma-separated)
- join_sql: corresponding join expressions (newline-separated or comma-separated, aligned to the secondary list)
- join_relationship: one_to_many or one_to_one (can be a single value reused for all listed joins)
- Optional per-cube view metadata in that first row:
  - view_name
  - view_title
  - view_description
  - visible_in_view
  - view_folder_name

Notes:
- The utility supports multiple values in “join_secondary_table” and “join_sql” as newline-separated or comma-separated lists.
- “cube_description” and “cube_title” from this first row will appear in the cube YAML.
- sql_table should be backtick-wrapped and is single-quoted in YAML output automatically.
- Dimensions can have “dimension_description” populated in the CSV; if you leave it blank, the utility auto-generates a helpful default.

Step 6) Build the view and update cubes from the edited CSV (repeat whenever you update the CSV joins or folder organization)
Run this right after CSV edits:
```bash
python3 utility.py --from-csv ./input/semantic_all.csv --output-dir ./output --view-name view_name (eg: influx_field_cassette) --view-root-cube cube_name(eg: influx_field_cassette_cell) --verbose
```

Outputs after Step 6:
- View YAML: ./output/views/influx_field_cassette.yml
- Cube YAMLs: ./output/cubes/<cube_name>.yml (updated from CSV)
- Logs: ./output/logs/log_YYYYMMDD_HHMMSS.md (includes the view YAML)

## YAML conventions enforced by the utility
- Cube header order: name, title, description, sql_table
- sql_table is wrapped in backticks and single-quoted in YAML
- Joins are read from the CSV’s first row per cube and written under the cube’s joins block
- Dimensions include title, description, sql, type, and primaryKey (if TRUE in CSV)
- Measures include title, description, sql, and type
- Default measure (if none specified) prefers plot_row_id for distinct count; otherwise uses the detected PK

## Example (macOS quick run after CSV edits)
```bash
export GOOGLE_APPLICATION_CREDENTIALS="/Users/ankitkumar/Library/CloudStorage/OneDrive-Bayer/Mac_OneDrive/myproject/breeding-cube-code/BigQuery/credentials.json"
cd "/Users/ankitkumar/Library/CloudStorage/OneDrive-Bayer/Mac_OneDrive/myproject/utilityUS/Semantic_Development_Utility"
python3 utility.py --from-csv ./input/semantic_all.csv --output-dir ./output --view-name influx_field_cassette --view-root-cube influx_field_cassette_cell --verbose
```

## Windows notes (if someone runs this on Windows)
- Set credentials (CMD):
```cmd
set GOOGLE_APPLICATION_CREDENTIALS=C:\path\to\service-account.json
```
- Change directory:
```cmd
cd C:\path\to\Semantic_Development_Utility
```
- Run the same commands but with python (not python3).

## Troubleshooting
- ModuleNotFoundError: No module named 'pandas'
  - Install packages in your active interpreter:
```bash
python -m pip install --upgrade pip
python -m pip install pandas google-cloud-bigquery google-cloud-datacatalog
```
- DefaultCredentialsError (no ADC):
  - Ensure GOOGLE_APPLICATION_CREDENTIALS points to a valid service account JSON with BigQuery Data Viewer (and optional Data Catalog Viewer).
- “Root cube not found for view generation”
  - Use --view-root-cube with your main cube name (must match cube_name in CSV and ./output/cubes filenames).
- Joins missing in view/cubes
  - Confirm join_secondary_table and join_sql are present in the first row per cube, using exact cube names and aligned lists (newline or comma-separated).
- Cube description not showing
  - Ensure cube_description is filled in the first row of that cube in the CSV; re-run Step 6.

## Run frequency guide
- Steps to run once per terminal session:
  - Set credentials
  - cd into project directory
- Steps to run once per machine or when missing packages:
  - Create/activate venv and install dependencies (or install packages with --user)
- Steps to run repeatedly:
  - Generate CSV + cubes from BigQuery (Step 4) when adding/updating source tables
  - Edit CSV (Step 5) to define joins, metadata, dimension/measures descriptions
  - Build view and update cubes from CSV (Step 6)
