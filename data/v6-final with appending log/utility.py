import argparse
import sys
import re
from pathlib import Path
from typing import Optional, Dict, List, Tuple
from datetime import datetime
import pandas as pd

# -----------------------------
# Utilities for cleaning values
# -----------------------------

def strip_outer_quotes(s: str) -> str:
    if s is None:
        return s
    s = s.strip()
    if not s:
        return s
    while len(s) >= 2 and (s[0] == s[-1]) and s[0] in ("'", '"'):
        s = s[1:-1].strip()
    return s

def clean_str(v):
    if pd.isna(v):
        return None
    s = str(v).strip()
    if s == "":
        return None
    s = strip_outer_quotes(s)
    return s if s != "" else None

def coerce_bool(v) -> Optional[bool]:
    if pd.isna(v):
        return None
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    if s == "":
        return None
    return s in ("true", "1", "yes", "y", "t")

def drop_empty_rows_and_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df2 = df.copy()
    df2 = df2.applymap(lambda x: None if (isinstance(x, str) and x.strip() == "") else x)
    df2 = df2.dropna(axis=1, how="all").dropna(axis=0, how="all")
    return df2

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    raw_cols = [str(c).strip().lower().replace(" ", "_") for c in out.columns]
    deduped = []
    seen = {}
    for c in raw_cols:
        if c not in seen:
            seen[c] = 1
            deduped.append(c)
        else:
            seen[c] += 1
            deduped.append(f"{c}_{seen[c]}")
    out.columns = deduped
    return out

# --------------------------------
# Column aliasing and section logic
# --------------------------------

REQUIRED: Dict[str, set] = {
    "cubes": {"table", "sql_table", "name"},
    "joins": {"primary_table", "secondary_table"},
    "dimensions": {"name", "sql", "type"},
    "measures": {"name", "sql", "type"},
}

ALIASES: Dict[str, Dict[str, str]] = {
    "cubes": {
        "table_name": "table",
        "cube_table": "table",
        "sqltable": "sql_table",
        "sql table": "sql_table",
        "cube_name": "name",
        "cube": "name",
        "desc": "description",
        "data source": "data_source",
        "data_source": "data_source",
    },
    "joins": {
        "primary table": "primary_table",
        "secondary table": "secondary_table",
        "relation": "relationship",
        "relationship_type": "relationship",
        "primary_table_key": "primary_table_key_column",
        "primary key column": "primary_table_key_column",
        "primary_key_column": "primary_table_key_column",
        "secondary_table_key": "secondary_table_key_column",
        "secondary key column": "secondary_table_key_column",
        "secondary_key_column": "secondary_table_key_column",
        "join_sql": "sql",
    },
    "dimensions": {
        "primary key": "primarykey",
        "primary_key": "primarykey",
        "is_primary_key": "primarykey",
        "pk": "primarykey",
        "datatype": "type",
        "data_type": "type",
        "cube": "cube",
        "cube_name": "cube",
    },
    "measures": {
        "aggregation": "type",
        "aggregate": "type",
        "agg": "type",
        "cube": "cube",
        "cube_name": "cube",
    },
}

def remap_known_columns(df: pd.DataFrame, section: str) -> pd.DataFrame:
    alias_map = ALIASES.get(section, {})
    rename_map = {}
    for c in df.columns:
        key = c.strip().lower()
        key_compact = key.replace(" ", "_")
        if key in alias_map:
            rename_map[c] = alias_map[key]
        elif key_compact in alias_map:
            rename_map[c] = alias_map[key_compact]
    return df.rename(columns=rename_map)

def score_section(df_cols: set, section: str, sheet_name: str) -> int:
    score = len(REQUIRED[section].intersection(df_cols))
    lname = sheet_name.lower()
    if section == "cubes" and "cube" in lname:
        score += 2
    if section == "joins" and ("join" in lname or "relationship" in lname):
        score += 2
    if section == "dimensions" and ("dimension" in lname or "dim" in lname):
        score += 2
    if section == "measures" and ("measure" in lname or "metric" in lname):
        score += 2
    return score

def detect_sections(xlsx_path: Path, verbose: bool = False) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    xl = pd.ExcelFile(xlsx_path, engine="openpyxl")

    cubes_df_list: List[pd.DataFrame] = []
    joins_df_list: List[pd.DataFrame] = []
    dims_df_list: List[pd.DataFrame] = []
    measures_df_list: List[pd.DataFrame] = []

    for sheet in xl.sheet_names:
        raw = pd.read_excel(xlsx_path, sheet_name=sheet, engine="openpyxl")
        raw = drop_empty_rows_and_columns(raw)
        if raw.empty:
            if verbose:
                print(f"[detect] Skip empty sheet: {sheet}")
            continue

        ndf = normalize_columns(raw)
        best_section = None
        best_score = -1
        candidates: Dict[str, pd.DataFrame] = {}
        for section in ("cubes", "joins", "dimensions", "measures"):
            remapped = remap_known_columns(ndf, section)
            candidates[section] = remapped
            sc = score_section(set(remapped.columns), section, sheet)
            if sc > best_score:
                best_score = sc
                best_section = section

        if best_section and best_score >= 2:
            chosen = candidates[best_section]
            if verbose:
                print(f"[detect] Sheet '{sheet}' => {best_section} (score={best_score})")
            if best_section == "cubes":
                cubes_df_list.append(chosen)
            elif best_section == "joins":
                joins_df_list.append(chosen)
            elif best_section == "dimensions":
                dims_df_list.append(chosen)
            elif best_section == "measures":
                measures_df_list.append(chosen)
        else:
            if verbose:
                print(f"[detect] Sheet '{sheet}' not confidently recognized (score={best_score}); ignoring.")

    cubes_df = pd.concat(cubes_df_list, ignore_index=True) if cubes_df_list else pd.DataFrame()
    joins_df = pd.concat(joins_df_list, ignore_index=True) if joins_df_list else pd.DataFrame()
    dims_df = pd.concat(dims_df_list, ignore_index=True) if dims_df_list else pd.DataFrame()
    measures_df = pd.concat(measures_df_list, ignore_index=True) if measures_df_list else pd.DataFrame()
    return cubes_df, joins_df, dims_df, measures_df

# --------------------------
# Struct building
# --------------------------

def row_to_dict(row: pd.Series) -> Dict[str, object]:
    out: Dict[str, object] = {}
    for k, v in row.items():
        if isinstance(v, str):
            vv = clean_str(v)
        elif isinstance(v, (int, float)) and pd.isna(v):
            vv = None
        else:
            vv = v if not pd.isna(v) else None
        if vv is not None:
            out[k] = vv
    return out

def make_one_line_description(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    s = s.replace("\r\n", " ").replace("\r", " ").replace("\n", " ")
    return ' '.join(s.split()).strip()

def build_sections(
    cubes_df: pd.DataFrame,
    joins_df: pd.DataFrame,
    dims_df: pd.DataFrame,
    measures_df: pd.DataFrame,
    only_cube: Optional[str] = None,
    include_unknown: bool = True
) -> List[Dict[str, object]]:
    # Build cubes with base fields
    cubes: List[Dict[str, object]] = []
    name_to_table: Dict[str, str] = {}
    cube_by_name: Dict[str, Dict[str, object]] = {}

    if not cubes_df.empty:
        for _, row in cubes_df.iterrows():
            r = row_to_dict(row)
            name = clean_str(r.get("name"))
            if only_cube and name != only_cube:
                continue
            table_name = clean_str(r.get("table"))
            if name and table_name:
                name_to_table[name] = table_name

            cube_obj: Dict[str, object] = {}
            # Known fields in desired order
            desc = r.get("description")
            if isinstance(desc, str):
                desc = make_one_line_description(desc)
            if desc is not None:
                cube_obj["description"] = desc
            if name is not None:
                cube_obj["name"] = name
            sql_tbl = r.get("sql_table")
            if isinstance(sql_tbl, str):
                sql_tbl = strip_outer_quotes(sql_tbl)
            if sql_tbl is not None:
                cube_obj["sql_table"] = sql_tbl
            title = r.get("title")
            if title is not None:
                cube_obj["title"] = title

            # Include any extra fields if requested
            if include_unknown:
                for k, v in r.items():
                    if k not in ("description", "name", "sql_table", "title", "table"):
                        cube_obj[k] = v

            # Initialize nested lists
            cube_obj["joins"] = []
            cube_obj["dimensions"] = []
            cube_obj["measures"] = []

            cubes.append(cube_obj)
            if name:
                cube_by_name[name] = cube_obj

    # Helper: choose target cube for a row
    def pick_target_cube_for_primary(primary_table: Optional[str]) -> Optional[Dict[str, object]]:
        if not cubes:
            return None
        # 1) If only_cube specified
        if only_cube and only_cube in cube_by_name:
            # Filter joins by primary table if provided
            if primary_table:
                allowed = {only_cube}
                if only_cube in name_to_table:
                    allowed.add(name_to_table[only_cube])
                if primary_table not in allowed:
                    return None
            return cube_by_name[only_cube]
        # 2) If exactly one cube, attach there
        if len(cubes) == 1:
            return cubes[0]
        # 3) Try match by primary_table with cube name or base table
        if primary_table:
            if primary_table in cube_by_name:
                return cube_by_name[primary_table]
            for cname, t in name_to_table.items():
                if t == primary_table:
                    return cube_by_name.get(cname)
        return None

    # Attach joins
    if not joins_df.empty:
        for _, row in joins_df.iterrows():
            r = row_to_dict(row)
            primary_table = clean_str(r.get("primary_table"))
            secondary_table = clean_str(r.get("secondary_table"))
            relationship = clean_str(r.get("relationship"))
            pk = clean_str(r.get("primary_table_key_column"))
            sk = clean_str(r.get("secondary_table_key_column"))
            sql_expr = clean_str(r.get("sql"))
            if not sql_expr and pk and sk:
                sql_expr = f"{pk}={sk}"

            target_cube = pick_target_cube_for_primary(primary_table)
            if target_cube is None:
                if len(cubes) != 1:
                    continue
                target_cube = cubes[0]

            join_obj: Dict[str, object] = {}
            if secondary_table is not None:
                join_obj["name"] = secondary_table
            if relationship is not None:
                join_obj["relationship"] = relationship
            if sql_expr is not None:
                join_obj["sql"] = sql_expr

            if include_unknown:
                for k, v in r.items():
                    if k not in ("primary_table", "secondary_table", "relationship", "primary_table_key_column", "secondary_table_key_column", "sql"):
                        join_obj[k] = v

            target_cube["joins"].append(join_obj)

    # Attach dimensions
    if not dims_df.empty:
        for _, row in dims_df.iterrows():
            r = row_to_dict(row)
            dim_cube_hint = clean_str(r.get("cube"))

            target_cube = None
            if only_cube and only_cube in cube_by_name:
                target_cube = cube_by_name[only_cube]
            elif len(cubes) == 1:
                target_cube = cubes[0]
            elif dim_cube_hint and dim_cube_hint in cube_by_name:
                target_cube = cube_by_name[dim_cube_hint]

            if target_cube is None:
                continue

            dim_obj: Dict[str, object] = {}
            if r.get("name") is not None:
                dim_obj["name"] = r.get("name")
            if r.get("title") is not None:
                dim_obj["title"] = r.get("title")
            desc = r.get("description")
            if isinstance(desc, str):
                desc = make_one_line_description(desc)
            if desc is not None:
                dim_obj["description"] = desc
            if r.get("sql") is not None:
                dim_obj["sql"] = r.get("sql")
            pk_val = coerce_bool(r.get("primarykey"))
            if pk_val is not None:
                dim_obj["primaryKey"] = bool(pk_val)
            if r.get("type") is not None:
                dim_obj["type"] = r.get("type")

            if include_unknown:
                for k, v in r.items():
                    if k not in ("name", "title", "description", "sql", "type", "primarykey", "cube"):
                        dim_obj[k] = v

            target_cube["dimensions"].append(dim_obj)

    # Attach measures
    if not measures_df.empty:
        for _, row in measures_df.iterrows():
            r = row_to_dict(row)
            meas_cube_hint = clean_str(r.get("cube"))

            target_cube = None
            if only_cube and only_cube in cube_by_name:
                target_cube = cube_by_name[only_cube]
            elif len(cubes) == 1:
                target_cube = cubes[0]
            elif meas_cube_hint and meas_cube_hint in cube_by_name:
                target_cube = cube_by_name[meas_cube_hint]

            if target_cube is None:
                continue

            meas_obj: Dict[str, object] = {}
            if r.get("name") is not None:
                meas_obj["name"] = r.get("name")
            if r.get("title") is not None:
                meas_obj["title"] = r.get("title")
            desc = r.get("description")
            if isinstance(desc, str):
                desc = make_one_line_description(desc)
            if desc is not None:
                meas_obj["description"] = desc
            if r.get("sql") is not None:
                meas_obj["sql"] = r.get("sql")
            if r.get("type") is not None:
                meas_obj["type"] = r.get("type")

            if include_unknown:
                for k, v in r.items():
                    if k not in ("name", "title", "description", "sql", "type", "cube"):
                        meas_obj[k] = v

            target_cube["measures"].append(meas_obj)

    return cubes

# --------------------------
# Manual YAML text rendering
# --------------------------

def dq(s: str) -> str:
    # Double-quoted YAML string with escaped quotes and single-line
    s = '' if s is None else str(s)
    s = s.replace("\r\n", " ").replace("\n", " ").replace("\r", " ")
    s = ' '.join(s.split())
    s = s.replace('"', '\\"')
    return f"\"{s}\""

def sq(s: str) -> str:
    # Single-quoted YAML string; escape single quotes by doubling
    s = '' if s is None else str(s)
    s = s.replace("'", "''")
    return f"'{s}'"

def is_simple_unquoted(s: Optional[str]) -> bool:
    if s is None:
        return False
    return bool(re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", str(s)))

def render_yaml_text(cubes: List[Dict[str, object]]) -> str:
    lines: List[str] = []
    indent2 = "  "
    indent4 = "    "

    lines.append("cubes:")
    for cube in cubes:
        # cube header
        lines.append("- " + (f'description: {dq(cube["description"])}' if cube.get("description") is not None else 'description: ""'))
        # name
        name = cube.get("name")
        if is_simple_unquoted(name):
            lines.append(f"{indent2}name: {name}")
        else:
            lines.append(f"{indent2}name: {dq(name) if name is not None else dq('')}")
        # sql_table
        sql_table = cube.get("sql_table")
        if sql_table is not None:
            lines.append(f"{indent2}sql_table: {sq(sql_table)}")
        # optional title
        title = cube.get("title")
        if title is not None:
            lines.append(f"{indent2}title: {dq(title)}")

        # blank line before joins
        if cube.get("joins"):
            lines.append("")
            lines.append(f"{indent2}joins:")
            for j in cube["joins"]:
                nm = j.get("name")
                lines.append(f"{indent2}- " + (f'name: {nm}' if is_simple_unquoted(nm) else f'name: {dq(nm)}'))
                if j.get("relationship") is not None:
                    lines.append(f"{indent4}relationship: {j['relationship']}")
                if j.get("sql") is not None:
                    lines.append(f"{indent4}sql: {sq(j['sql'])}")

        # blank line before dimensions
        if cube.get("dimensions"):
            lines.append("")
            lines.append(f"{indent2}dimensions:")
            lines.append(f"{indent2}#----------joining keys--------------")
            for d in cube["dimensions"]:
                nm = d.get("name")
                lines.append(f"{indent2}- " + (f'name: {nm}' if is_simple_unquoted(nm) else f'name: {dq(nm)}'))
                if d.get("title") is not None:
                    lines.append(f"{indent4}title: {dq(d['title'])}")
                if d.get("description") is not None:
                    lines.append(f"{indent4}description: {dq(d['description'])}")
                if d.get("sql") is not None:
                    lines.append(f"{indent4}sql: {sq(d['sql'])}")
                if "primaryKey" in d and d["primaryKey"] is not None:
                    lines.append(f"{indent4}primaryKey: {'true' if bool(d['primaryKey']) else 'false'}")
                if d.get("type") is not None:
                    tval = d["type"]
                    if is_simple_unquoted(tval):
                        lines.append(f"{indent4}type: {tval}")
                    else:
                        lines.append(f"{indent4}type: {dq(tval)}")

        # blank line before measures
        if cube.get("measures"):
            lines.append("")
            lines.append(f"{indent2}measures:")
            for m in cube["measures"]:
                nm = m.get("name")
                lines.append(f"{indent2}- " + (f'name: {nm}' if is_simple_unquoted(nm) else f'name: {dq(nm)}'))
                if m.get("title") is not None:
                    lines.append(f"{indent4}title: {dq(m['title'])}")
                if m.get("description") is not None:
                    lines.append(f"{indent4}description: {dq(m['description'])}")
                if m.get("sql") is not None:
                    lines.append(f"{indent4}sql: {sq(m['sql'])}")
                if m.get("type") is not None:
                    tval = m["type"]
                    if is_simple_unquoted(tval):
                        lines.append(f"{indent4}type: {tval}")
                    else:
                        lines.append(f"{indent4}type: {dq(tval)}")

    return "\n".join(lines) + "\n"

# --------------------------
# Log file creation (new per run)
# --------------------------

DEFAULT_LOG_MD_TEMPLATE = """### Overview
This log documents the development and progress of the Excel to YAML (Semantic) Utility project. The utility converts Excel templates into structured YAML files for data modeling and semantic design.

--- ## Log Entries

#
- **Initial Setup**
  - Created the project repository.
  - Set up the development environment with Python 3.9+.
  - Installed required packages: `pandas`, `openpyxl`, `PyYAML`.

#
- **Feature Implementation**
  - Developed core functionality to read Excel files using `pandas`.
  - Implemented normalization of column names to ensure consistency across different sheets.
  - Created functions to handle cubes, joins, dimensions, and measures.

#
- **YAML Generation**
  - Implemented logic to convert the structured data into YAML format.
  - Ensured proper indentation and formatting for readability.
  - Added functionality to handle optional parameters for filtering cubes.

#
- **Command-Line Interface**
  - Developed a command-line interface (CLI) using `argparse` for user interaction.
  - Added options for input and output file paths, filtering by cube, and verbose output.

#
- **Testing and Validation**
  - Validated the output YAML files against sample input Excel templates.
  - Fixed issues related to formatting and unnecessary spaces in descriptions.

#
- **Documentation**
  - Created a README file detailing the utility's features, usage, and installation instructions.
  - Documented the structure of the Excel template and the expected output format.

#
- **Final Review**
  - Reviewed the code for optimization and readability.
  - Ensured all features are functioning correctly and documentation is complete.
  - Prepared for project handoff or deployment.

--- ## Output

The generated YAML file from the utility currently looks like this:

```yaml
{YAML_CONTENT}
```

--- ## Next Steps
- Gather user feedback on the utility.
- Plan for future enhancements based on user requirements.
- Consider adding support for additional output formats (e.g., JSON).

--- ## Notes
- Ensure to regularly update this log with new developments, issues encountered, and resolutions.
- Keep track of any changes made to the requirements or scope of the project.

---
"""

def create_new_log(logs_dir: Path, yaml_text: str) -> Path:
    logs_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    log_filename = f"log_{timestamp}.md"
    log_path = logs_dir / log_filename
    content = DEFAULT_LOG_MD_TEMPLATE.replace("{YAML_CONTENT}", yaml_text.strip())
    log_path.write_text(content, encoding="utf-8")
    return log_path

# ---------------
# Command-line app
# ---------------

def main():
    """
    CLI entry point.
    Example:
      python excel_to_yaml.py -i ./input/template.xlsx -o ./output/capacity_request.yml --only-cube capacity_request --verbose
    """
    parser = argparse.ArgumentParser(description="Convert Excel semantic template to YAML text with nested joins/dimensions/measures under cubes.")
    parser.add_argument("-i", "--input", required=True, help="Path to input Excel file (.xlsx)")
    parser.add_argument("-o", "--output", required=True, help="Path to output YAML file (.yml)")
    parser.add_argument("--only-cube", help="If set, include only this cube by name and filter joins/dimensions/measures accordingly.")
    parser.add_argument("--no-include-unknown", action="store_true", help="Do not include unknown/extra columns in the output YAML.")
    parser.add_argument("--verbose", action="store_true", help="Print detection details.")
    args = parser.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)

    # Determine logs directory at the same level as 'input' and 'output' folders
    # If output path is like <root>/output/*.yml, use <root>/logs
    if output_path.parent.name.lower() == "output":
        logs_dir = output_path.parent.parent / "logs"
    else:
        # Fallback: sibling 'logs' next to output file's parent
        logs_dir = output_path.parent / "logs"

    if not input_path.exists():
        print(f"Error: Input file not found: {input_path}", file=sys.stderr)
        sys.exit(1)

    output_path.parent.mkdir(parents=True, exist_ok=True)

    cubes_df, joins_df, dims_df, measures_df = detect_sections(input_path, verbose=args.verbose)
    if cubes_df.empty and joins_df.empty and dims_df.empty and measures_df.empty:
        print("Error: No recognizable sections found. Check your headers or use --verbose to see detection details.", file=sys.stderr)
        sys.exit(1)

    cubes = build_sections(
        cubes_df=cubes_df,
        joins_df=joins_df,
        dims_df=dims_df,
        measures_df=measures_df,
        only_cube=args.only_cube,
        include_unknown=(not args.no_include_unknown),
    )

    yaml_text = render_yaml_text(cubes)

    # Write YAML text to output file
    output_path.write_text(yaml_text, encoding="utf-8")
    print(f"Wrote YAML to: {output_path}")

    # Create a NEW logs/log_YYYYMMDD_HHMMSS.md file with latest output
    new_log_path = create_new_log(logs_dir, yaml_text)
    print(f"Created log file: {new_log_path}")

if __name__ == "__main__":
    main()
