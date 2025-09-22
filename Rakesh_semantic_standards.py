#!/usr/bin/env python3
"""
Utility to validate YAML files against semantic standards.

Currently supported tools:
 - CUBE: Validates cube YAML files with the following checks:

Checks (per cube):
 - Name check: cube object has non-empty 'name'
 - Description check: cube object has non-empty 'description'
 - sql_table check: cube object has non-empty 'sql_table'
 - Title check: cube object has non-empty 'title'
 - Filename match check: cube name matches YAML filename stem
 - Unique cube name check: cube name not used as (a) another file's stem or (b) another cube's name in same folder

Checks (per dimension and measure inside each cube):
 - name (key 'name')
 - title (key 'title')
 - description (key 'description')
 - type (key 'type')
 - sql: must have '{CUBE}' before any '.'
Each must be present and non-empty (string for name/title/description, any non-empty value for type treated as string).
Both dimensions and measures must have these fields.

Output format example:
    Filename: inventory.yml
    Cube name: inventory
    Name check: PASS
    Description check: PASS
    sql_table check: PASS
    Title check: FAIL
    Filename match check: PASS
    Unique cube name check: PASS
    Dimensions:
      dimension 'br_field_id':
        name check: PASS
        title check: FAIL
        description check: FAIL
        type check: PASS
      ...

Usage:
    python semantic_standards.py <tool> <yaml_file>

Example:
    python semantic_standards.py CUBE cube-models/cubes/inventory.yml
"""

from __future__ import annotations
import sys
import datetime
import os
from pathlib import Path
from typing import List, Dict, Any, Set, TextIO

try:
    import yaml  # type: ignore
except Exception:  # pragma: no cover
    yaml = None


def load_yaml(path: Path) -> Any:
    if yaml is None:
        raise RuntimeError("PyYAML not installed. Install with: pip install pyyaml")
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def get_cubes(data: Any) -> List[Dict[str, Any]]:
    if not isinstance(data, dict):
        return []
    cubes = data.get("cubes")
    if not isinstance(cubes, list):
        return []
    result: List[Dict[str, Any]] = []
    for item in cubes:
        if isinstance(item, dict):
            result.append(item)
    return result


def gather_other_names(current_file: Path) -> (Set[str], Set[str]):
    """
    Return (other_file_stems, other_cube_names) for all *.yml / *.yaml
    files in the same directory excluding the current file.
    """
    dir_path = current_file.parent
    file_stems: Set[str] = set()
    cube_names: Set[str] = set()
    for f in dir_path.iterdir():
        if f == current_file or not f.is_file() or f.suffix.lower() not in {".yml", ".yaml"}:
            continue
        file_stems.add(f.stem)
        try:
            data = load_yaml(f)
        except Exception:
            continue
        for cube in get_cubes(data):
            name_val = cube.get("name")
            if isinstance(name_val, str) and name_val.strip():
                cube_names.add(name_val)
    return file_stems, cube_names


def validate_sql_format(sql_val: Any) -> bool:
    """Validate that SQL field has {CUBE} before any '.'"""
    if not isinstance(sql_val, str) or not sql_val.strip():
        return False
    sql = sql_val.strip()
    dot_idx = sql.find('.')
    if dot_idx == -1:
        return False
    prefix = sql[:dot_idx]
    return "{CUBE}" in prefix


def validate_measures(cube: Dict[str, Any]) -> List[str]:
    """
    Produce validation output lines for each measure inside a cube.
    """
    lines: List[str] = []
    measures = cube.get("measures")
    if not isinstance(measures, list):
        lines.append("  (No 'measures' list found)")
        return lines

    for measure in measures:
        if not isinstance(measure, dict):
            lines.append("  (Invalid measure entry - not a mapping)")
            continue
        measure_name = measure.get("name")
        name_ok = isinstance(measure_name, str) and measure_name.strip() != ""
        title_val = measure.get("title")
        title_ok = isinstance(title_val, str) and title_val.strip() != ""
        desc_val = measure.get("description")
        desc_ok = isinstance(desc_val, str) and desc_val.strip() != ""
        type_val = measure.get("type")
        type_ok = isinstance(type_val, str) and type_val.strip() != ""
        sql_val = measure.get("sql")
        sql_ok = validate_sql_format(sql_val)  # No need to pass cube_name anymore
        label = measure_name if name_ok else "(missing name)"
        lines.append(f"  measure '{label}':")
        lines.append(f"    name check: {'PASS' if name_ok else 'FAIL'}")
        lines.append(f"    title check: {'PASS' if title_ok else 'FAIL'}")
        lines.append(f"    description check: {'PASS' if desc_ok else 'FAIL'}")
        lines.append(f"    type check: {'PASS' if type_ok else 'FAIL'}")
        lines.append(f"    sql format check: {'PASS' if sql_ok else 'FAIL'}")
    return lines


def validate_dimensions(cube: Dict[str, Any]) -> List[str]:
    """
    Produce validation output lines for each dimension inside a cube.
    """
    lines: List[str] = []
    dims = cube.get("dimensions")
    if not isinstance(dims, list):
        lines.append("  (No 'dimensions' list found)")
        return lines

    for dim in dims:
        if not isinstance(dim, dict):
            lines.append("  (Invalid dimension entry - not a mapping)")
            continue
        dim_name = dim.get("name")
        name_ok = isinstance(dim_name, str) and dim_name.strip() != ""
        title_val = dim.get("title")
        title_ok = isinstance(title_val, str) and title_val.strip() != ""
        desc_val = dim.get("description")
        desc_ok = isinstance(desc_val, str) and desc_val.strip() != ""
        type_val = dim.get("type")
        type_ok = isinstance(type_val, str) and type_val.strip() != ""
        sql_val = dim.get("sql")
        sql_ok = validate_sql_format(sql_val)  # No need to pass cube_name anymore
        label = dim_name if name_ok else "(missing name)"
        lines.append(f"  dimension '{label}':")
        lines.append(f"    name check: {'PASS' if name_ok else 'FAIL'}")
        lines.append(f"    title check: {'PASS' if title_ok else 'FAIL'}")
        lines.append(f"    description check: {'PASS' if desc_ok else 'FAIL'}")
        lines.append(f"    type check: {'PASS' if type_ok else 'FAIL'}")
        lines.append(f"    sql format check: {'PASS' if sql_ok else 'FAIL'}")
    return lines


def validate_cube_file(path: Path) -> None:
    """Validate a YAML file according to cube standards"""
    try:
        data = load_yaml(path)
    except Exception as e:
        print(f"ERROR: cannot parse {path.name}: {e}", file=sys.stderr)
        return

    cubes = get_cubes(data)
    if not cubes:
        print(f"FAIL: no cube objects found in {path.name}")
        return

    other_file_stems, other_cube_names = gather_other_names(path)

    # Process each cube
    for idx, cube in enumerate(cubes):
        name_val = cube.get("name")
        desc_val = cube.get("description")
        sql_table_val = cube.get("sql_table")
        title_val = cube.get("title")

        stem = path.stem

        name_ok = isinstance(name_val, str) and name_val.strip() != ""
        desc_ok = isinstance(desc_val, str) and desc_val.strip() != ""
        sql_table_ok = isinstance(sql_table_val, str) and sql_table_val.strip() != ""
        title_ok = isinstance(title_val, str) and title_val.strip() != ""
        filename_match_ok = name_ok and (name_val == stem)

        unique_ok = True
        reason = ""
        if name_ok:
            if name_val in other_file_stems:
                unique_ok = False
                reason = "name matches another file name"
            elif name_val in other_cube_names:
                unique_ok = False
                reason = "name matches another cube name"

        if len(cubes) > 1:
            print(f"Cube index: {idx}")
        print(f" **********  Standards Check Summary Result  ********** ")
        print(f"Filename: {path.name}")
        print(f"Cube name: {name_val if name_ok else '(missing)'}")
        
        # Overall standards check - FAIL if any check fails
        standards_pass = all([
            name_ok,
            desc_ok,
            sql_table_ok,
            title_ok,
            filename_match_ok,
            unique_ok
        ])
        

        print(f"Standards Check Final Result: {'PASS' if standards_pass else 'FAIL'}\n")

        print(f" **********  Standards Check Detailed Result ********** ")
        
        print(f"Name check: {'PASS' if name_ok else 'FAIL'}")
        print(f"Description check: {'PASS' if desc_ok else 'FAIL'}")
        print(f"sql_table check: {'PASS' if sql_table_ok else 'FAIL'}")
        print(f"Title check: {'PASS' if title_ok else 'FAIL'}")
        print(f"Filename match check: {'PASS' if filename_match_ok else 'FAIL'}")
        if unique_ok:
            print("Unique cube name check: PASS")
        else:
            print(f"Unique cube name check: FAIL ({reason})")

        # Dimension validations
        print("Dimensions:")
        for line in validate_dimensions(cube):
            print(line)
            
        # Measure validations
        print("\nMeasures:")
        for line in validate_measures(cube):
            print(line)


def get_log_filename(yaml_file: str) -> str:
    """Generate log filename with timestamp"""
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    base_name = Path(yaml_file).stem
    return f"logs/log_semantic_standards_{base_name}_{timestamp}.txt"


class TeeOutput:
    """Write output to both console and log file"""
    def __init__(self, log_file: TextIO):
        self.console = sys.stdout
        self.log_file = log_file

    def write(self, message: str):
        self.console.write(message)
        self.log_file.write(message)

    def flush(self):
        self.console.flush()
        self.log_file.flush()


def validate_file(tool: str, yaml_file: str) -> None:
    """Dispatch validation based on tool type"""
    path = Path(yaml_file)
    if not path.is_file():
        print(f"ERROR: file not found: {yaml_file}", file=sys.stderr)
        return

    # Create logs directory if it doesn't exist
    os.makedirs("logs", exist_ok=True)
    
    # Setup logging
    log_path = get_log_filename(yaml_file)
    with open(log_path, 'w', encoding='utf-8') as log_file:
        # Redirect stdout to both console and log file
        sys.stdout = TeeOutput(log_file)
        
        try:
            if tool.upper() == "CUBE":
                validate_cube_file(path)
            else:
                print(f"ERROR: Unsupported tool '{tool}'. Currently only CUBE is supported.", file=sys.stderr)
        finally:
            # Restore stdout
            sys.stdout = sys.__stdout__


def main(argv: List[str]) -> int:
    if len(argv) != 2:
        print("Usage: python semantic_standards.py <tool> <yaml_file>", file=sys.stderr)
        print("\nSupported tools:\n  CUBE - validate cube YAML files", file=sys.stderr)
        return 2
        
    tool, yaml_file = argv
    validate_file(tool, yaml_file)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
