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
    acronyms = {"id", "api", "url", "ip", "uuid", "ssn", "dna", "rna", "ui", "db", "rm"}
    titled = []
    for p in parts:
        lp = p.lower()
        if lp in acronyms:
            titled.append(lp.upper())
        else:
            titled.append(lp[:1].upper() + lp[1:])
    return " ".join(titled)

def parse_multi_cell(cell: Optional[str]) -> List[str]:
    """
    Parse multi-value cells which may contain newline-separated or comma-separated items,
    optionally quoted. Returns a list of trimmed tokens.
    """
    s = clean_str(cell)
    if not s:
        return []
    parts = []
    for line in str(s).splitlines():
        for token in re.split(r",", line):
            tok = strip_outer_quotes(token).strip()
            if tok:
                parts.append(tok)
    return parts

def auto_dimension_description(cube_name: str, column_name: str, column_title: str) -> str:
    """
    Generate a helpful default description for dimensions.
    """
    # Special-case some common IDs for a slightly richer text
    if column_name.lower() in ("plot_row_id", "cassette_bid", "cell_number"):
        base = f"{column_title}"
        return f"Unique identifier or key for {base.lower()} in {titleize_identifier(cube_name)}. Records reflect planning/allocation details where applicable."
    return f"The {column_title} dimension from {titleize_identifier(cube_name)}; values sourced from '{column_name}'."

# --------------------------
# Log file creation
# --------------------------

DEFAULT_LOG_MD_TEMPLATE_MULTI = """### BigQuery to Semantic Run

Timestamp: {TS}

Processed cubes:
{CUBE_LIST}

--- ## Generated Cube YAMLs

{CUBE_YAML_BLOCKS}

--- ## Generated View YAMLs

{VIEW_YAML_BLOCKS}

--- ## Notes
- Primary key detected from Data Catalog tags or column descriptions; falls back to naming heuristics.
- Default measure is count_distinct of the selected identifier (prefers plot_row_id, else detected PK).
- Views are generated in --from-csv mode after you add join conditions in semantic_all.csv.
"""

def create_new_log_multi(logs_dir: Path, yamls_by_cube: Dict[str, str], yamls_by_view: Dict[str, str], error_log_path: Optional[Path] = None) -> Path:
    logs_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    log_filename = f"log_{timestamp}.md"
    log_path = logs_dir / log_filename

    cube_list = "\n".join([f"- {c}" for c in sorted(yamls_by_cube.keys())]) or "(none)"
    cube_yaml_blocks = []
    for c, y in yamls_by_cube.items():
        cube_yaml_blocks.append(f"### {c}\n\n```yaml\\n{y.strip()}\\n```")
    view_yaml_blocks = []
    for v, y in yamls_by_view.items():
        view_yaml_blocks.append(f"### {v}\n\n```yaml\\n{y.strip()}\\n```")
    if error_log_path:
        view_yaml_blocks.append(f"\n---\nErrors were captured in: {error_log_path}\n")

    content = DEFAULT_LOG_MD_TEMPLATE_MULTI.format(
        TS=timestamp,
        CUBE_LIST=cube_list,
        CUBE_YAML_BLOCKS="\n\n".join(cube_yaml_blocks) if cube_yaml_blocks else "(none)",
        VIEW_YAML_BLOCKS="\n\n".join(view_yaml_blocks) if view_yaml_blocks else "(none)",
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
        lines.append(f"=== Resource: {cube} ===")
        lines.append(err_text)
        lines.append("")
    err_path.write_text("\n".join(lines), encoding="utf-8")
    return err_path

# --------------------------
# Manual YAML text rendering (cubes) with required header order
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
        # Header order: name, title, description, sql_table
        name = cube.get("name")
        lines.append("- " + (f"name: {name}" if is_simple_unquoted(name) else f"name: {dq(name) if name is not None else dq('')}"))
        title = cube.get("title")
        if title is not None:
            lines.append(f"{indent2}title: {dq(title)}")
        desc = cube.get("description")
        if desc is not None:
            lines.append(f"{indent2}description: {dq(desc)}")
        sql_table = cube.get("sql_table")
        if sql_table is not None:
            lines.append(f"{indent2}sql_table: {sq(sql_table)}")

        # joins
        if cube.get("joins"):
            lines.append("")
            lines.append(f"{indent2}joins:")
            for j in cube["joins"]:
                nm = j.get("name")
                lines.append(f"{indent2}- " + (f"name: {nm}" if is_simple_unquoted(nm) else f"name: {dq(nm)}"))
                if j.get("relationship") is not None:
                    lines.append(f"{indent4}relationship: {j['relationship']}")
                if j.get("sql") is not None:
                    lines.append(f"{indent4}sql: {sq(j['sql'])}")

        # dimensions
        if cube.get("dimensions"):
            lines.append("")
            lines.append(f"{indent2}dimensions:")
            for d in cube["dimensions"]:
                nm = d.get("name")
                lines.append(f"{indent2}- " + (f"name: {nm}" if is_simple_unquoted(nm) else f"name: {dq(nm)}"))
                if d.get("title") is not None:
                    lines.append(f"{indent4}title: {dq(d['title'])}")
                if d.get("description") is not None:
                    lines.append(f"{indent4}description: {dq(d['description'])}")
                if d.get("sql") is not None:
                    lines.append(f"{indent4}sql: {sq(d['sql'])}")
                if d.get("type") is not None:
                    tval = d["type"]
                    if is_simple_unquoted(tval):
                        lines.append(f"{indent4}type: {tval}")
                    else:
                        lines.append(f"{indent4}type: {dq(tval)}")
                if "primaryKey" in d and d["primaryKey"] is not None:
                    lines.append(f"{indent4}primaryKey: {'true' if bool(d['primaryKey']) else 'false'}")

        # measures
        if cube.get("measures"):
            lines.append("")
            lines.append(f"{indent2}measures:")
            for m in cube["measures"]:
                nm = m.get("name")
                lines.append(f"{indent2}- " + (f"name: {nm}" if is_simple_unquoted(nm) else f"name: {dq(nm)}"))
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
# View YAML rendering
# --------------------------

def render_view_yaml(view_obj: Dict[str, object]) -> str:
    lines: List[str] = []
    indent2 = "  "
    indent4 = "    "
    indent6 = "      "
    indent8 = "        "
    indent10 = "          "

    lines.append("views:")
    lines.append(f"{indent2}- name: {view_obj['name']}")
    lines.append(f"{indent4}title: {dq(view_obj['title'])}")
    lines.append(f"{indent4}description: {dq(view_obj['description'])}")

    # cubes section
    lines.append(f"{indent4}cubes:")
    for jp in view_obj["cubes"]:
        lines.append(f"{indent6}- join_path: {jp['join_path']}")
        lines.append(f"{indent8}includes: ")
        for inc in jp["includes"]:
            if isinstance(inc, dict) and inc.get("commented"):
                name = inc.get("name", "")
                reason = inc.get("reason")
                if reason:
                    lines.append(f"{indent10}# - {name}  # {reason}")
                else:
                    lines.append(f"{indent10}# - {name}  # duplicate")
            else:
                lines.append(f"{indent10}- {inc}")

    # folders section
    if view_obj.get("folders"):
        lines.append("")
        lines.append(f"{indent4}folders : ")
        for folder in view_obj["folders"]:
            lines.append(f"{indent6}- name : {folder['name']}")
            lines.append(f"{indent8}includes : ")
            for inc in folder["includes"]:
                if isinstance(inc, dict) and inc.get("commented"):
                    name = inc.get("name", "")
                    reason = inc.get("reason")
                    if reason:
                        lines.append(f"{indent10}# - {name}  # {reason}")
                    else:
                        lines.append(f"{indent10}# - {name}  # duplicate")
                else:
                    lines.append(f"{indent10}- {inc}")

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

# --------------------------
# Schema flattening for nested RECORD fields
# --------------------------

def flatten_schema(fields: List[bigquery.SchemaField], parent: Optional[str] = None) -> List[Tuple[str, bigquery.SchemaField]]:
    """
    Recursively flatten BigQuery schema. Returns a list of (dot_path, leaf_field).
    - dot_path uses parent.child notation suitable for use in SQL as {CUBE}.<dot_path>
    - Only non-RECORD leaves are returned. RECORD fields are traversed into their subfields.
    - REPEATED leaves are included as-is (no UNNEST handling), matching current modeling approach.
    """
    out: List[Tuple[str, bigquery.SchemaField]] = []
    for f in fields:
        current_path = f"{parent}.{f.name}" if parent else f.name
        # Traverse nested RECORDs
        if f.field_type.upper() == "RECORD" and getattr(f, "fields", None):
            try:
                children = list(f.fields)  # type: ignore
            except Exception:
                children = []
            if children:
                out.extend(flatten_schema(children, current_path))
            else:
                # Empty RECORD: nothing to emit
                continue
        else:
            out.append((current_path, f))
    return out


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
# CSV creation and YAML build (cubes)
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
    cube_sql_table = f"`{project}.{dataset}.{table}`"  # backtick-wrapped
    cube_title_auto = titleize_identifier(cube_name) if not cube_title_override else cube_title_override
    cube_desc_final = cube_description or None
    cube_data_source = extract_data_source_from_project(project)

    # Nested-aware primary key detection using flattened schema
    flattened = flatten_schema(schema)
    path_to_leaf: Dict[str, str] = {p: f.name for (p, f) in flattened}
    leaf_names: List[str] = [f.name for (_, f) in flattened]

    pk_path: Optional[str] = None
    pk_leaf: Optional[str] = None

    # Try Data Catalog tags first
    try:
        project, dataset, table_name = project, dataset, table  # already parsed above
        entry = lookup_datacatalog_entry_linked(project, dataset, table_name, verbose=verbose)
        pk_cols_dc = extract_pk_from_datacatalog(entry, verbose=verbose)
        # Match DC-tagged column against flattened paths first, then leaf names
        for col in pk_cols_dc:
            if col in path_to_leaf:
                pk_path = col
                pk_leaf = path_to_leaf[col]
                break
        if not pk_leaf and pk_cols_dc:
            # Fallback: if DC provided only a leaf name, align to first matching leaf
            for col in pk_cols_dc:
                for p, lf in path_to_leaf.items():
                    if lf == col:
                        pk_path = p
                        pk_leaf = lf
                        break
                if pk_leaf:
                    break
    except Exception:
        pass

    # Next, column descriptions indicating PK on leaves
    if not pk_leaf:
        for p, f in flattened:
            desc = (getattr(f, "description", None) or "").lower()
            if "primary key" in desc or re.search(r"\bpk\b", desc):
                pk_path = p
                pk_leaf = f.name
                break

    # Heuristics on leaf names
    if not pk_leaf:
        table_exact = f"{table}_id"
        # exact leaf match like table_id
        for p, f in flattened:
            if f.name == table_exact:
                pk_path, pk_leaf = p, f.name
                break
    if not pk_leaf:
        # first leaf ending with _id
        for p, f in flattened:
            if f.name.lower().endswith("_id"):
                pk_path, pk_leaf = p, f.name
                break
    if not pk_leaf:
        # plain 'id'
        for p, f in flattened:
            if f.name.lower() == "id":
                pk_path, pk_leaf = p, f.name
                break

    # Prefer plot_row_id as default distinct measure key if present, else PK leaf
    preferred_distinct_name: Optional[str] = None
    names_lower = {name.lower(): name for name in leaf_names}
    if "plot_row_id" in names_lower:
        preferred_distinct_name = names_lower["plot_row_id"]
    elif pk_leaf:
        preferred_distinct_name = pk_leaf

    rows: List[Dict[str, object]] = []

    def bq_type_to_semantic(ftype: str) -> str:
        return BQS_TYPE_TO_SEMANTIC.get(ftype.upper(), "string")

    # Flatten nested RECORD fields into dot-paths
    flattened = flatten_schema(schema)
    for path, field in flattened:
        leaf_name = field.name
        dim_title_auto = titleize_identifier(leaf_name)
        dim_desc_auto = auto_dimension_description(cube_name, leaf_name, dim_title_auto)
        rows.append({
            "dimension_name": leaf_name,
            "dimension_measure_flag": "dimension",
            "dimension_title": dim_title_auto,
            "dimension_description": dim_desc_auto,
            "dimension_sql": f"{{CUBE}}.{path}",
            "primary_key": "TRUE" if ((pk_path and path == pk_path) or (pk_leaf and leaf_name == pk_leaf)) else "",
            "dimension_type": bq_type_to_semantic(field.field_type),
            "cube_name": cube_name,
            "cube_sql_table": cube_sql_table,
            "cube_description": cube_desc_final,
            "cube_title": cube_title_auto,
            "cube_data_source": cube_data_source,
            # view-related fields left empty; you will fill joins later
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

    # Default measure: distinct count of the detected key; use PK dot-path when available
    if preferred_distinct_name:
        # Map leaf names to their first dot-path for nested fields
        leaf_to_path: Dict[str, str] = {}
        for p, f in flattened:
            leaf_to_path.setdefault(f.name, p)
        # Prefer PK leaf for measure naming; fallback to preferred_distinct_name
        measure_source_name = pk_leaf or preferred_distinct_name
        # Build SQL using PK path if present; else use dot-path of the chosen leaf; else fallback to dimension reference
        measure_path = pk_path or leaf_to_path.get(measure_source_name)
        pd_title = titleize_identifier(measure_source_name)
        measure_title = f"Distinct count of {pd_title}"
        measure_desc = f"This is to get Distinct count of {pd_title} recorded."
        rows.append({
            "dimension_name": f"count_distinct_{measure_source_name}",
            "dimension_measure_flag": "measure",
            "dimension_title": measure_title,
            "dimension_description": measure_desc,
            "dimension_sql": (f"{{CUBE}}.{measure_path}" if measure_path else f"{{{measure_source_name}}}"),
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
            "name": cube_name,
            "title": cube_title,
            "description": cube_desc,
            "sql_table": cube_sql_table,
        }

        cube["joins"] = []
        cube["dimensions"] = []
        cube["measures"] = []

        # Joins (read from the first row per cube; supports multi-line lists)
        join_cols = ["join_primary_table", "join_secondary_table", "join_sql", "join_relationship"]
        if all(col in gdf.columns for col in join_cols):
            jrow = None
            for _, jr in gdf.iterrows():
                if clean_str(jr.get("join_secondary_table")) or clean_str(jr.get("join_sql")):
                    jrow = jr
                    break
            if jrow is not None:
                secondaries = parse_multi_cell(jrow.get("join_secondary_table"))
                sqls = parse_multi_cell(jrow.get("join_sql"))
                relationships = parse_multi_cell(jrow.get("join_relationship"))
                max_len = max(len(secondaries), len(sqls))
                if max_len == 0 and (clean_str(jrow.get("join_secondary_table")) or clean_str(jrow.get("join_sql"))):
                    sec = clean_str(jrow.get("join_secondary_table"))
                    sq = clean_str(jrow.get("join_sql"))
                    rel = clean_str(jrow.get("join_relationship"))
                    if sec or sq or rel:
                        j = {}
                        if sec: j["name"] = sec
                        if rel: j["relationship"] = rel
                        if sq: j["sql"] = sq
                        cube["joins"].append(j)
                else:
                    if len(secondaries) < max_len:
                        secondaries += [""] * (max_len - len(secondaries))
                    if len(sqls) < max_len:
                        sqls += [""] * (max_len - len(sqls))
                    if len(relationships) < max_len and relationships:
                        relationships += [relationships[-1]] * (max_len - len(relationships))
                    for i in range(max_len):
                        sec = clean_str(secondaries[i]) if i < len(secondaries) else None
                        sq = clean_str(sqls[i]) if i < len(sqls) else None
                        rel = clean_str(relationships[i]) if i < len(relationships) and relationships else None
                        if sec or sq or rel:
                            j = {}
                            if sec: j["name"] = sec
                            if rel: j["relationship"] = rel
                            if sq: j["sql"] = sq
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

def write_yaml_per_cube(cubes: List[Dict[str, object]], cubes_dir: Path) -> Dict[str, str]:
    yamls_by_cube: Dict[str, str] = {}
    cubes_dir.mkdir(parents=True, exist_ok=True)
    for cube in cubes:
        yaml_text = render_yaml_text([cube])
        name = cube.get("name") or "cube"
        yamls_by_cube[name] = yaml_text
        out_path = cubes_dir / f"{name}.yml"
        out_path.write_text(yaml_text, encoding="utf-8")
        print(f"Wrote Cube YAML to: {out_path}")
    return yamls_by_cube

# --------------------------
# View building (from CSV joins + cube metadata)
# --------------------------

def derive_view_metadata_from_csv(df: pd.DataFrame, fallback_name: str) -> Tuple[str, str, str]:
    """
    Determine view name, title, description from CSV if present.
    """
    vname = None
    vtitle = None
    vdesc = None
    for _, r in df.iterrows():
        vn = clean_str(r.get("view_name"))
        vt = clean_str(r.get("view_title"))
        vd = make_one_line_description(clean_str(r.get("view_description")))
        if vn and not vname:
            vname = vn
        if vt and not vtitle:
            vtitle = vt
        if vd and not vdesc:
            vdesc = vd
        if vname and vtitle and vdesc:
            break
    vname = vname or fallback_name
    vtitle = vtitle or titleize_identifier(vname)
    vdesc = vdesc or f"Auto-generated view based on CSV joins and cubes. Root cube is auto-selected."
    return vname, vtitle, vdesc

def resolve_root_cube(requested: Optional[str], cube_by_name: Dict[str, Dict[str, object]], edges: Dict[str, Set[str]]) -> Optional[str]:
    if requested and requested in cube_by_name:
        return requested
    if requested:
        if ("dim_" + requested) in cube_by_name:
            return "dim_" + requested
        for name in cube_by_name.keys():
            if name.startswith("dim_") and name[len("dim_"):] == requested:
                return name
    candidates = [n for n in edges.keys() if n in cube_by_name]
    if candidates:
        best = max(candidates, key=lambda n: len(edges.get(n, set())))
        return best
    if "product" in cube_by_name:
        return "product"
    if "dim_product" in cube_by_name:
        return "dim_product"
    return sorted(cube_by_name.keys())[0] if cube_by_name else None

def build_view_from_csv_and_cubes(
    csv_path: Path,
    cubes: List[Dict[str, object]],
    view_name_arg: str,
    root_cube: Optional[str] = None,
    view_description: Optional[str] = None
) -> Dict[str, object]:
    df = pd.read_csv(csv_path)
    df = df.dropna(how="all")

    # Determine view metadata from CSV if supplied, else fallback to args
    view_name, view_title, view_desc = derive_view_metadata_from_csv(df, view_name_arg)

    # Build adjacency from CSV join rows (support multiline join values in first row per cube)
    edges: Dict[str, Set[str]] = {}
    for cube_name, gdf in df.groupby("cube_name", dropna=True):
        jrow = None
        for _, jr in gdf.iterrows():
            if clean_str(jr.get("join_secondary_table")) or clean_str(jr.get("join_sql")):
                jrow = jr
                break
        if jrow is None:
            continue
        primary = clean_str(jrow.get("join_primary_table")) or cube_name
        secondaries = parse_multi_cell(jrow.get("join_secondary_table"))
        if secondaries:
            edges.setdefault(primary, set()).update(secondaries)

    cube_by_name: Dict[str, Dict[str, object]] = {c["name"]: c for c in cubes if c.get("name")}

    resolved_root = resolve_root_cube(root_cube, cube_by_name, edges)
    if not resolved_root or resolved_root not in cube_by_name:
        raise ValueError("Root cube not found for view generation.")

    def fields_for_cube(c: Dict[str, object]) -> List[str]:
        inc: List[str] = []
        for d in c.get("dimensions", []):
            if d.get("name"):
                inc.append(d["name"])
        for m in c.get("measures", []):
            if m.get("name"):
                inc.append(m["name"])
        return inc

    # Build join_paths via DFS
    join_paths: List[Dict[str, object]] = []
    visited_paths: Set[Tuple[str, ...]] = set()
    # Global dedupe across the entire view: comment duplicates instead of removing
    global_seen: Set[str] = set()
    global_origin: Dict[str, str] = {}

    # Root cube includes with comments for duplicates
    root_includes_all = fields_for_cube(cube_by_name[resolved_root])
    root_includes_repr: List[object] = []
    for x in root_includes_all:
        if x not in global_seen:
            root_includes_repr.append(x)
            global_seen.add(x)
            global_origin[x] = resolved_root
        else:
            root_includes_repr.append({"name": x, "commented": True, "reason": f"duplicate of {global_origin.get(x, 'previous cube')}"})
    join_paths.append({"join_path": resolved_root, "includes": root_includes_repr})
    visited_paths.add((resolved_root,))

    def dfs(current: str, path: List[str]):
        neighbors = sorted([n for n in edges.get(current, set()) if n in cube_by_name])
        for n in neighbors:
            new_path = path + [n]
            tpl = tuple(new_path)
            if tpl in visited_paths:
                continue
            visited_paths.add(tpl)
            jp = ".".join(new_path)
            incs_all = fields_for_cube(cube_by_name[n])
            incs_repr: List[object] = []
            for x in incs_all:
                if x not in global_seen:
                    incs_repr.append(x)
                    global_seen.add(x)
                    global_origin[x] = n
                else:
                    incs_repr.append({"name": x, "commented": True, "reason": f"duplicate of {global_origin.get(x, 'previous cube')}"})
            join_paths.append({"join_path": jp, "includes": incs_repr})
            dfs(n, new_path)

    dfs(resolved_root, [resolved_root])

    # Folders: comment off duplicates within each folder; align with view join_paths
    folders: List[Dict[str, object]] = []

    # Map cube -> folder name (CSV override if present)
    folder_name_by_cube: Dict[str, str] = {}
    for cube_name, gdf in df.groupby("cube_name", dropna=True):
        folder_name = None
        for _, r in gdf.iterrows():
            fn = clean_str(r.get("view_folder_name"))
            if fn:
                folder_name = fn
                break
        folder_name_by_cube[cube_name] = folder_name or titleize_identifier(cube_name)

    # Build folder includes from join_paths, marking duplicates as comments
    folder_includes_map: Dict[str, List[object]] = {}
    folder_seen: Dict[str, Set[str]] = {}

    for jp in join_paths:
        target_cube = jp["join_path"].split(".")[-1]
        folder = folder_name_by_cube.get(target_cube, titleize_identifier(target_cube))
        fi_list = folder_includes_map.setdefault(folder, [])
        seen_set = folder_seen.setdefault(folder, set())
        for inc in jp["includes"]:
            if isinstance(inc, dict) and inc.get("commented"):
                fi_list.append({"name": inc.get("name", ""), "commented": True, "reason": inc.get("reason")})
                continue
            name = inc if isinstance(inc, str) else inc.get("name")
            if not name:
                continue
            if name in seen_set:
                fi_list.append({"name": name, "commented": True, "reason": "duplicate in folder"})
            else:
                fi_list.append(name)
                seen_set.add(name)

    for fname, includes in folder_includes_map.items():
        folders.append({"name": fname, "includes": includes})

    return {"name": view_name, "title": view_title, "description": view_desc, "cubes": join_paths, "folders": folders}

def write_view_yaml(view_obj: Dict[str, object], views_dir: Path) -> Dict[str, str]:
    views_dir.mkdir(parents=True, exist_ok=True)
    yaml_text = render_view_yaml(view_obj)
    out_path = views_dir / f"{view_obj['name']}.yml"
    out_path.write_text(yaml_text, encoding="utf-8")
    print(f"Wrote View YAML to: {out_path}")
    return {view_obj["name"]: yaml_text}

# ---------------
# Command-line app
# ---------------

def main():
    parser = argparse.ArgumentParser(
        description="Generate/append semantic CSV and Cube YAMLs from BigQuery tables, and/or build Cube + View YAMLs from an edited semantic CSV."
    )
    parser.add_argument("--bq-table", action="append", help="BigQuery table in project.dataset.table form. Can be passed multiple times.")
    parser.add_argument("--bq-tables", help="Comma-separated BigQuery tables in project.dataset.table form.")
    parser.add_argument("--from-csv", help="Path to the edited semantic CSV file to build Cube and View YAMLs")
    parser.add_argument("-i", "--input-dir", default="./input", help="Folder where the semantic CSV will be written/updated")
    parser.add_argument("--output-dir", default="./output", help="Folder where YAML files will be written (cubes/ and views/ subfolders)")
    parser.add_argument("--view-name", default="deployments", help="Name of the view YAML to generate (used if CSV lacks a view_name)")
    parser.add_argument("--view-root-cube", help="Root cube name for nested join_paths in the view")
    parser.add_argument("--cube-title", help="Optional cube title override (applies to all cubes in this run; otherwise auto-title from cube_name)")
    parser.add_argument("--cube-description", help="Optional cube description to store in CSV/YAML (applies to all cubes in this run)")
    parser.add_argument("--verbose", action="store_true", help="Print details")
    args = parser.parse_args()

    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)
    cubes_dir = output_dir / "cubes"
    views_dir = output_dir / "views"

    # Determine logs directory at the same level as 'output' folder
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

    # Mode: Build Cube + View from edited CSV (updates cubes too)
    if args.from_csv and not tables:
        csv_path = Path(args.from_csv)
        if not csv_path.exists():
            print(f"Error: CSV file not found: {csv_path}", file=sys.stderr)
            sys.exit(1)
        # Rebuild cubes from CSV and write cube YAMLs
        cubes = build_cubes_from_semantic_csv(csv_path, only_cubes=None)
        yamls_by_cube = write_yaml_per_cube(cubes, cubes_dir)
        # Build and write view (using CSV-provided metadata if present)
        try:
            view_obj = build_view_from_csv_and_cubes(csv_path, cubes, args.view_name, root_cube=args.view_root_cube, view_description=None)
            yamls_by_view = write_view_yaml(view_obj, views_dir)
            err_log_path = None
            log_path = create_new_log_multi(logs_dir, yamls_by_cube, yamls_by_view, err_log_path)
            print(f"Created log file: {log_path}")
        except Exception as e:
            err_text = f"Exception while building view from CSV '{csv_path}': {e}\n{traceback.format_exc()}"
            err_log_path = create_error_log(logs_dir, [(args.view_name, err_text)])
            print(err_text, file=sys.stderr)
            print(f"Errors captured in: {err_log_path}")
        return

    # Mode: BigQuery -> append/update CSV + write Cube YAMLs (no View generation)
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
    yamls_by_cube = write_yaml_per_cube(cubes, cubes_dir)

    err_log_path = create_error_log(logs_dir, errors)
    log_path = create_new_log_multi(logs_dir, yamls_by_cube, {}, err_log_path)
    print(f"Created log file: {log_path}")
    if err_log_path:
        print(f"Errors captured in: {err_log_path}")

if __name__ == "__main__":
    main()