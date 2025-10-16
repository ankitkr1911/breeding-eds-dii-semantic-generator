# Excel to YAML (Semantic) Utility

## Overview

The Excel to YAML (Semantic) Utility is a powerful tool designed to convert Excel templates into structured YAML files. This utility is particularly useful for data modeling and semantic design, allowing users to define cubes, joins, dimensions, and measures in an Excel format and seamlessly translate that into a YAML configuration.

## Features

This utility reads an Excel template containing the following sheets:

### 1. Cubes
- **table**: The name of the underlying table.
- **sql_table**: The SQL representation of the table.
- **name**: The name of the cube.
- **description**: A detailed description of the cube's purpose and functionality.
- **title**: The title for display purposes.

### 2. Joins
- **Primary Table**: The main table involved in the join.
- **Secondary Table**: The table that is being joined to the primary table.
- **relationship**: The nature of the relationship (e.g., one-to-many).
- **Primary Table Key Column**: The key column in the primary table.
- **Secondary Table Key Column**: The key column in the secondary table.

### 3. Dimensions
- **name**: The name of the dimension.
- **title**: A title for the dimension.
- **description**: A description detailing the dimension's role.
- **sql**: The SQL representation of the dimension.
- **primaryKey**: Indicates if the dimension is a primary key.
- **type**: The data type of the dimension.

### 4. Measures
- **name**: The name of the measure.
- **title**: A title for the measure.
- **description**: A description of the measure's purpose.
- **sql**: The SQL representation of the measure.
- **type**: The aggregation type (e.g., sum, average).

## Usage

### 1. Excel -> YAML
To convert an Excel template to a YAML file, use the command line interface:

```bash
python utility.py -i "input/Semantic_design_template.xlsx" -o "output/semantic_output.yml"
```

### 2. BigQuery Table -> CSV + YAML
You can also generate a semantic design directly from a BigQuery table (schema introspection). This will:
- Infer a cube whose name is the table name (last segment).
- Create a CSV scaffold in `input/semantic_<table_name>1.csv` listing all columns as dimensions.
- Infer a primary key using heuristic order: `<table>_id`, `id`, first column ending `_id`, else first required column.
- Add a default measure `distinct_<pk>_count` with `type=countDistinct` (if a PK was inferred).
- Generate the YAML in the specified `-o` path (e.g. `output/semantic_output.yml`).
- Create a new timestamped log file in `logs/`.

Example:
```bash
python utility.py --bq-table bcs-breeding-datasets.velocity.capacity_request -o output/semantic_output.yml
```

Resulting CSV pattern (example `capacity_request`):
```
input/semantic_capacity_request1.csv
```

### Command-Line Options
Mutually exclusive input modes:
- `-i`, `--input`: Path to the input Excel file (.xlsx).
- `--bq-table`: Fully qualified BigQuery table (`project.dataset.table` or `dataset.table` if default project configured).

Common:
- `-o`, `--output`: Path to the output YAML file (.yml).
- `--only-cube`: (Excel mode) Filter to a single cube name.
- `--no-include-unknown`: (Excel mode) Exclude extra/unrecognized columns from YAML.
- `--verbose`: (Excel mode) Print sheet detection diagnostics.

### BigQuery Notes
Authentication relies on your local gcloud / ADC configuration (e.g. `gcloud auth application-default login` or `GOOGLE_APPLICATION_CREDENTIALS`).
If a table has no obvious primary key, the first required column is used; adjust manually afterward in the generated CSV/YAML. Joins are intentionally omitted for manual editing later.

## Requirements

Python 3.9+ and these packages:
- `pandas` – data manipulation
- `openpyxl` – Excel reading
- `PyYAML` – YAML (currently manual rendering used, but retained)
- `google-cloud-bigquery` – BigQuery metadata access (only needed for `--bq-table` mode)

Install:
```bash
pip install pandas openpyxl PyYAML google-cloud-bigquery
```

## Conclusion

The Excel to YAML (Semantic) Utility is an essential tool for anyone looking to streamline their data modeling process. By converting Excel templates into structured YAML files, this utility facilitates easier integration into data processing pipelines and enhances overall productivity.

For any issues or suggestions, please feel free to open an issue in the repository.

---
