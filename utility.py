import argparse
import sys
import re
import traceback
from pathlib import Path
from typing import Optional, Dict, List, Tuple, Set
from datetime import datetime

import pandas as pd
from google.cloud import bigquery

# Data Catalog is optional; handle gracefully if not installed
try:
    from google.cloud import datacatalog_v1
    DATACATALOG_AVAILABLE = True
except Exception:
    DATACATALOG_AVAILABLE = False

# -----------------------------
# Utilities for cleaning/formatting
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
    if v is None:
        return None
    try:
        import math
        if isinstance(v, float) and math.isnan(v):
            return None
    except Exception:
        pass
    s = str(v).strip()
    if s == "":
        return None
    s = strip_outer_quotes(s)
    return s if s != "" else None

def make_one_line_description(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    s = s.replace("\r\n", " ").replace("\r", " ").replace("\n", " ")
    return " ".join(s.split()).strip()

def titleize_identifier(name: str) -> str:
    if not name:
        return ""
    parts = re.split(r"[_\s]+", name.strip())
    acronyms = {"id", "api", "url", "ip", "uuid", "ssn", "dna", "rna", "ui", "db"}
    titled = []
    for p in parts:
        lp = p.lower()
        if lp in acronyms:
            titled.append(lp.upper())
        else:
            titled.append(lp[:1].upper() + lp[1:])
    return " ".join(titled)

# --------------------------
# Log file creation
# --------------------------

DEFAULT_LOG_MD_TEMPLATE_MULTI = """### BigQuery to Semantic Run

Timestamp: {TS}

Processed cubes:
{CUBE_LIST}

--- ## Generated YAMLs

{YAML_BLOCKS}

--- ## Notes
- Primary key detected from Data Catalog tags or column descriptions; falls back to naming heuristics.
- Default measure is count_distinct of the detected primary key.
- Joins and Views can be added in the CSV and re-applied via --from-csv mode.
"""

def create_new_log_multi(logs_dir: Path, yamls_by_cube: Dict[str, str], error_log_path: Optional[Path] = None) -> Path:
    logs_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    log_filename = f"log_{timestamp}.md"
    log_path = logs_dir / log_filename

    cube_list = "\n".join([f"- {c}" for c in sorted(yamls_by_cube.keys())]) or "(none)"
    yaml_blocks = []
    for c, y in yamls_by_cube.items():
        yaml_blocks.append(f"### {c}\n\n```yaml\n{y.strip()}\n```")
    if error_log_path:
        yaml_blocks.append(f"\n---\nErrors were captured in: {error_log_path}\n")

    content = DEFAULT_LOG_MD_TEMPLATE_MULTI.format(
        TS=timestamp,
        CUBE_LIST=cube_list,
        YAML_BLOCKS="\n\n".join(yaml_blocks),
    )
    log_path.write_text(content, encoding="utf-8")
    return log_path

def create_error_log(logs_dir: Path, errors: List[Tuple[str, str]]) -> Optional[Path]:
    if not errors:
        return None
    err_dir = logs_dir / "errors"
    err_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    err_path = err_dir / f"errors_{timestamp}.log"
    lines = []
    for cube, err_text in errors:
        lines.append(f"=== Cube: {cube} ===")
        lines.append(err_text)
        lines.append("")
    err_path.write_text("\n".join(lines), encoding="utf-8")
    return err_path

# --------------------------
# Manual YAML text rendering
# --------------------------

def dq(s: str) -> str:
    s = '' if s is None else str(s)
    s = s.replace("\r\n", " ").replace("\n", " ").replace("\r", " ")
    s = ' '.join(s.split())
    s = s.replace('"', '\\"')
    return f"\"{s}\""

def sq(s: str) -> str:
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
        lines.append("- " + (f'description: {dq(cube["description"])}' if cube.get("description") is not None else 'description: ""'))
        name = cube.get("name")
        if is_simple_unquoted(name):
            lines.append(f"{indent2}name: {name}")
        else:
            lines.append(f"{indent2}name: {dq(name) if name is not None else dq('')}")
        sql_table = cube.get("sql_table")
        if sql_table is not None:
            lines.append(f"{indent2}sql_table: {sq(sql_table)}")
        title = cube.get("title")
        if title is not None:
            lines.append(f"{indent2}title: {dq(title)}")

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
# BigQuery and Data Catalog helpers
# --------------------------

SEMANTIC_CSV_HEADERS = [
    "dimension_name", "dimension_measure_flag", "dimension_title", "dimension_description",
    "dimension_sql", "primary_key", "dimension_type",
    "cube_name", "cube_sql_table", "cube_description", "cube_title", "cube_data_source",
    "view_name", "view_title", "view_description", "visible_in_view", "view_folder_name",
    "join_primary_table", "join_secondary_table", "join_sql", "join_relationship",
]

BQS_TYPE_TO_SEMANTIC = {
    "STRING": "string", "BYTES": "string",
    "INT64": "number", "INTEGER": "number",
    "FLOAT64": "number", "FLOAT": "number",
    "NUMERIC": "number", "BIGNUMERIC": "number",
    "BOOL": "boolean", "BOOLEAN": "boolean",
    "TIMESTAMP": "time", "DATE": "time", "DATETIME": "time", "TIME": "time",
    "GEOGRAPHY": "string",
}

def parse_bq_table_id(table_id: str) -> Tuple[str, str, str]:
    parts = table_id.split(".")
    if len(parts) != 3:
        raise ValueError(f"BigQuery table must be in 'project.dataset.table' form: {table_id}")
    return parts[0], parts[1], parts[2]

def extract_data_source_from_project(project: str) -> str:
    tokens = project.split("-")
    if len(tokens) >= 2:
        return tokens[1]
    return project

def fetch_bq_schema(table_id: str, verbose: bool = False) -> List[bigquery.SchemaField]:
    client = bigquery.Client()
    tbl = client.get_table(table_id)
    if verbose:
        print(f"[bq] Loaded table: {tbl.full_table_id} with {len(tbl.schema)} columns")
    return tbl.schema

def lookup_datacatalog_entry_linked(project: str, dataset: str, table: str, verbose: bool = False):
    if not DATACATALOG_AVAILABLE:
        if verbose:
            print("[dc] Data Catalog client not available; skipping PK tag lookup.")
        return None
    client = datacatalog_v1.DataCatalogClient()
    linked_resource = f"//bigquery.googleapis.com/projects/{project}/datasets/{dataset}/tables/{table}"
    try:
        entry = client.lookup_entry(request={"linked_resource": linked_resource})
        if verbose:
            print(f"[dc] Found Data Catalog entry: {entry.name}")
        return entry
    except Exception as e:
        if verbose:
            print(f"[dc] No Data Catalog entry found for {linked_resource}: {e}")
        return None

def extract_pk_from_datacatalog(entry, verbose: bool = False) -> Set[str]:
    pk_cols: Set[str] = set()
    if entry is None or not DATACATALOG_AVAILABLE:
        return pk_cols
    client = datacatalog_v1.DataCatalogClient()
    try:
        for tag in client.list_tags(parent=entry.name):
            def field_truthy(tag_fields: Dict[str, datacatalog_v1.TagField]) -> bool:
                for k, tf in tag_fields.items():
                    lk = k.lower()
                    if lk in ("primary_key", "is_pk", "pk"):
                        if getattr(tf, "bool_value", False) is True:
                            return True
                        sv = (getattr(tf, "string_value", "") or "").strip().lower()
                        if sv in ("true", "yes", "y", "1"):
                            return True
                        enum = getattr(tf, "enum_value", None)
                        if enum and enum.display_name and enum.display_name.lower() in ("true", "yes", "pk"):
                            return True
                return False

            try:
                fields = tag.fields
            except Exception:
                fields = {}
            if field_truthy(fields):
                col = tag.column.strip() if getattr(tag, "column", None) else None
                if col:
                    pk_cols.add(col)
                else:
                    if verbose:
                        print("[dc] PK tag at table level; column unspecified. Ignoring.")
    except Exception as e:
        if verbose:
            print(f"[dc] Error reading tags: {e}")
    return pk_cols

def detect_primary_key_columns(table_id: str, schema: List[bigquery.SchemaField], verbose: bool = False) -> List[str]:
    project, dataset, table = parse_bq_table_id(table_id)

    entry = lookup_datacatalog_entry_linked(project, dataset, table, verbose=verbose)
    pk_cols = extract_pk_from_datacatalog(entry, verbose=verbose)
    if pk_cols:
        if verbose:
            print(f"[pk] Data Catalog tags indicate PK columns: {sorted(pk_cols)}")
        return sorted(pk_cols)

    desc_pks: List[str] = []
    for f in schema:
        desc = (getattr(f, "description", None) or "").lower()
        if "primary key" in desc or re.search(r"\bpk\b", desc):
            desc_pks.append(f.name)
    if desc_pks:
        if verbose:
            print(f"[pk] Column descriptions indicate PK columns: {desc_pks}")
        return desc_pks

    heuristics: List[str] = []
    exact = f"{table}_id"
    for f in schema:
        if f.name == exact:
            heuristics = [f.name]
            break
    if not heuristics:
        ends = [f.name for f in schema if f.name.endswith("_id")]
        if ends:
            heuristics = [ends[0]]
    if not heuristics:
        ids = [f.name for f in schema if f.name.lower() == "id"]
        if ids:
            heuristics = [ids[0]]

    if verbose:
        print(f"[pk] Heuristic PK columns: {heuristics if heuristics else 'NONE'}")
    return heuristics

# --------------------------
# CSV creation and YAML build
# --------------------------

def generate_rows_for_table(
    table_id: str,
    cube_title_override: Optional[str] = None,
    cube_description: Optional[str] = None,
    verbose: bool = False,
) -> Tuple[str, List[Dict[str, object]]]:
    project, dataset, table = parse_bq_table_id(table_id)
    schema = fetch_bq_schema(table_id, verbose=verbose)
    cube_name = table
    cube_sql_table = f"{project}.{dataset}.{table}"
    cube_title_auto = titleize_identifier(cube_name) if not cube_title_override else cube_title_override
    cube_desc_final = cube_description or None
    cube_data_source = extract_data_source_from_project(project)

    pk_cols = detect_primary_key_columns(table_id, schema, verbose=verbose)
    pk = pk_cols[0] if pk_cols else None

    rows: List[Dict[str, object]] = []

    def bq_type_to_semantic(ftype: str) -> str:
        return BQS_TYPE_TO_SEMANTIC.get(ftype.upper(), "string")

    for field in schema:
        dim_title_auto = titleize_identifier(field.name)
        rows.append({
            "dimension_name": field.name,
            "dimension_measure_flag": "dimension",
            "dimension_title": dim_title_auto,
            "dimension_description": None,
            "dimension_sql": f"{{CUBE}}.{field.name}",
            "primary_key": "TRUE" if (pk and field.name == pk) else "",
            "dimension_type": bq_type_to_semantic(field.field_type),
            "cube_name": cube_name,
            "cube_sql_table": cube_sql_table,
            "cube_description": cube_desc_final,
            "cube_title": cube_title_auto,
            "cube_data_source": cube_data_source,
            "view_name": None,
            "view_title": None,
            "view_description": None,
            "visible_in_view": None,
            "view_folder_name": None,
            "join_primary_table": cube_name,
            "join_secondary_table": None,
            "join_sql": None,
            "join_relationship": None,
        })

    # Default measure: distinct count of PK with descriptive text and dimension-style SQL reference
    if pk:
        pk_title = titleize_identifier(pk)
        measure_title = f"Distinct count of {pk_title}"
        # If dataset is present, mention it as "<dataset> application"
        app_phrase = f" recorded in the {dataset} application" if dataset else ""
        measure_desc = f"This is to get Distinct count of {pk_title}{app_phrase}"
        rows.append({
            "dimension_name": f"count_distinct_{pk}",
            "dimension_measure_flag": "measure",
            "dimension_title": measure_title,
            "dimension_description": measure_desc,
            "dimension_sql": f"{{{pk}}}",  # reference the dimension, not {CUBE}.col
            "primary_key": "",
            "dimension_type": "count_distinct",
            "cube_name": cube_name,
            "cube_sql_table": cube_sql_table,
            "cube_description": cube_desc_final,
            "cube_title": cube_title_auto,
            "cube_data_source": cube_data_source,
            "view_name": None,
            "view_title": None,
            "view_description": None,
            "visible_in_view": None,
            "view_folder_name": None,
            "join_primary_table": cube_name,
            "join_secondary_table": None,
            "join_sql": None,
            "join_relationship": None,
        })

    return cube_name, rows

def upsert_rows_into_csv(csv_path: Path, rows: List[Dict[str, object]], cube_name: str) -> None:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    df_new = pd.DataFrame(rows, columns=SEMANTIC_CSV_HEADERS)
    if csv_path.exists():
        df = pd.read_csv(csv_path)
        if not set(SEMANTIC_CSV_HEADERS).issubset(set(df.columns)):
            df = pd.DataFrame(columns=SEMANTIC_CSV_HEADERS)
        df = df[df["cube_name"] != cube_name]
        df_all = pd.concat([df, df_new], ignore_index=True)
    else:
        df_all = df_new
    df_all = df_all.reindex(columns=SEMANTIC_CSV_HEADERS)
    df_all.to_csv(csv_path, index=False)

def build_cubes_from_semantic_csv(csv_path: Path, only_cubes: Optional[Set[str]] = None) -> List[Dict[str, object]]:
    df = pd.read_csv(csv_path)
    df = df.dropna(how="all")
    cubes: List[Dict[str, object]] = []

    if "cube_name" not in df.columns:
        return cubes

    grouped = df.groupby("cube_name", dropna=True)
    for cube_name, gdf in grouped:
        if only_cubes and cube_name not in only_cubes:
            continue

        cube_sql_table = clean_str(gdf["cube_sql_table"].dropna().iloc[0]) if "cube_sql_table" in gdf.columns else None
        cube_title = clean_str(gdf["cube_title"].dropna().iloc[0]) if "cube_title" in gdf.columns and not gdf["cube_title"].dropna().empty else None
        cube_desc = make_one_line_description(clean_str(gdf["cube_description"].dropna().iloc[0])) if "cube_description" in gdf.columns and not gdf["cube_description"].dropna().empty else None

        cube: Dict[str, object] = {
            "description": cube_desc or "",
            "name": cube_name,
            "sql_table": cube_sql_table,
        }
        if cube_title:
            cube["title"] = cube_title
        cube["joins"] = []
        cube["dimensions"] = []
        cube["measures"] = []

        # Joins
        join_cols = ["join_primary_table", "join_secondary_table", "join_sql", "join_relationship"]
        if all(col in gdf.columns for col in join_cols):
            jdf = gdf.copy()
            jdf["join_secondary_table"] = jdf["join_secondary_table"].apply(clean_str)
            jdf["join_sql"] = jdf["join_sql"].apply(clean_str)
            jdf["join_relationship"] = jdf["join_relationship"].apply(clean_str)
            jdf = jdf.dropna(subset=["join_secondary_table", "join_sql"], how="all")
            seen = set()
            for _, jr in jdf.iterrows():
                sec = jr.get("join_secondary_table")
                sql = jr.get("join_sql")
                rel = jr.get("join_relationship")
                key = (sec or "", sql or "", rel or "")
                if not any([sec, sql, rel]):
                    continue
                if key in seen:
                    continue
                seen.add(key)
                j = {}
                if sec: j["name"] = sec
                if rel: j["relationship"] = rel
                if sql: j["sql"] = sql
                cube["joins"].append(j)

        # Dimensions and measures
        for _, r in gdf.iterrows():
            flag = clean_str(r.get("dimension_measure_flag"))
            name = clean_str(r.get("dimension_name"))
            title = clean_str(r.get("dimension_title"))
            desc = make_one_line_description(clean_str(r.get("dimension_description")))
            sql = clean_str(r.get("dimension_sql"))
            typ = clean_str(r.get("dimension_type"))
            pk = clean_str(r.get("primary_key"))

            if flag == "dimension":
                dim = {"name": name}
                if title: dim["title"] = title
                if desc: dim["description"] = desc
                if sql: dim["sql"] = sql
                if typ: dim["type"] = typ
                if pk and pk.upper() == "TRUE":
                    dim["primaryKey"] = True
                cube["dimensions"].append(dim)

            elif flag == "measure":
                meas = {"name": name}
                if title: meas["title"] = title
                if desc: meas["description"] = desc
                if sql: meas["sql"] = sql
                if typ: meas["type"] = typ
                cube["measures"].append(meas)

        cubes.append(cube)

    return cubes

def write_yaml_per_cube(cubes: List[Dict[str, object]], output_dir: Path) -> Dict[str, str]:
    yamls_by_cube: Dict[str, str] = {}
    output_dir.mkdir(parents=True, exist_ok=True)
    for cube in cubes:
        name = cube.get("name")
        yaml_text = render_yaml_text([cube])
        yamls_by_cube[name] = yaml_text
        out_path = output_dir / f"{name}.yml"
        out_path.write_text(yaml_text, encoding="utf-8")
        print(f"Wrote YAML to: {out_path}")
    return yamls_by_cube

# ---------------
# Command-line app
# ---------------

def main():
    parser = argparse.ArgumentParser(
        description="Generate/append semantic CSV and parallel YAMLs from one or more BigQuery tables, or rebuild YAMLs from an existing semantic CSV."
    )
    # Multiple tables: pass --bq-table multiple times or use --bq-tables comma-separated
    parser.add_argument("--bq-table", action="append", help="BigQuery table in project.dataset.table form. Can be passed multiple times.")
    parser.add_argument("--bq-tables", help="Comma-separated BigQuery tables in project.dataset.table form.")
    parser.add_argument("--from-csv", help="Path to an existing semantic CSV file to build YAMLs for all cubes found")
    parser.add_argument("-i", "--input-dir", default="./input", help="Folder where the semantic CSV will be written/updated")
    parser.add_argument("--output-dir", default="./output", help="Folder where YAML files will be written")
    parser.add_argument("--cube-title", help="Optional cube title override (applies to all cubes in this run; otherwise auto-title from cube_name)")
    parser.add_argument("--cube-description", help="Optional cube description to store in CSV/YAML (applies to all cubes in this run)")
    parser.add_argument("--verbose", action="store_true", help="Print details")
    args = parser.parse_args()

    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)

    # Determine logs directory at the same level as 'input' and 'output' folders
    if output_dir.name.lower() == "output":
        logs_dir = output_dir.parent / "logs"
    else:
        logs_dir = output_dir / "logs"

    # Collect table list
    tables: List[str] = []
    if args.bq_table:
        tables.extend(args.bq_table)
    if args.bq_tables:
        for t in args.bq_tables.split(","):
            t = t.strip()
            if t:
                tables.append(t)

    # Mode: from existing CSV (rebuild YAMLs for all cubes in CSV)
    if args.from_csv and not tables:
        csv_path = Path(args.from_csv)
        if not csv_path.exists():
            print(f"Error: CSV file not found: {csv_path}", file=sys.stderr)
            sys.exit(1)
        cubes = build_cubes_from_semantic_csv(csv_path, only_cubes=None)
        yamls_by_cube = write_yaml_per_cube(cubes, output_dir)
        err_log_path = None
        log_path = create_new_log_multi(logs_dir, yamls_by_cube, err_log_path)
        print(f"Created log file: {log_path}")
        return

    # Mode: BigQuery -> append/update CSV + write YAMLs for provided tables
    if not tables:
        print("Error: Provide either --from-csv or one/more tables via --bq-table/--bq-tables", file=sys.stderr)
        sys.exit(1)

    combined_csv_path = input_dir / "semantic_all.csv"
    errors: List[Tuple[str, str]] = []
    processed_cubes: List[str] = []

    for table_id in tables:
        try:
            cube_name, rows = generate_rows_for_table(
                table_id=table_id,
                cube_title_override=args.cube_title,
                cube_description=args.cube_description,
                verbose=args.verbose,
            )
            upsert_rows_into_csv(combined_csv_path, rows, cube_name)
            processed_cubes.append(cube_name)
            print(f"Upserted CSV rows for cube: {cube_name}")
        except Exception as e:
            err_text = f"Exception while processing '{table_id}': {e}\n{traceback.format_exc()}"
            print(err_text, file=sys.stderr)
            errors.append((table_id, err_text))

    if not combined_csv_path.exists():
        print("Error: Combined CSV not created; no successful tables processed.", file=sys.stderr)
        sys.exit(1)

    only_set = set(processed_cubes) if processed_cubes else None
    cubes = build_cubes_from_semantic_csv(combined_csv_path, only_cubes=only_set)
    yamls_by_cube = write_yaml_per_cube(cubes, output_dir)

    err_log_path = create_error_log(logs_dir, errors)
    log_path = create_new_log_multi(logs_dir, yamls_by_cube, err_log_path)
    print(f"Created log file: {log_path}")
    if err_log_path:
        print(f"Errors captured in: {err_log_path}")

if __name__ == "__main__":
    main()