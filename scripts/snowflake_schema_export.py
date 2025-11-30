from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import List

# Ensure repo root is on sys.path so `src` package can be imported when running script
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.snowflake_schema import SnowflakeSchemaExporter


def parse_types(s: str) -> List[str]:
    return [p.strip() for p in s.split(",") if p.strip()]


def main(argv=None):
    parser = argparse.ArgumentParser(description="Export Snowflake schema-only DDL to files")
    parser.add_argument("--database", help="Snowflake database (overrides config)")
    parser.add_argument("--schema", help="Snowflake schema (overrides config)")
    parser.add_argument("--output-dir", default="schema_export", help="Directory to write SQL files")
    parser.add_argument("--types", default="tables,views,materialized_views,sequences,file_formats",
                        help="Comma-separated object types to export (tables,views,materialized_views,sequences,file_formats)")

    args = parser.parse_args(argv)

    types = parse_types(args.types)
    exporter = SnowflakeSchemaExporter(database=args.database, schema=args.schema)
    try:
        exported = exporter.export_schema(args.output_dir, object_types=types)
    except Exception as e:
        print(f"Error exporting schema: {e}")
        return 2

    total = sum(len(v) for v in exported.values())
    print(f"Exported {total} objects into '{args.output_dir}'")
    for k, v in exported.items():
        print(f"- {k}: {len(v)} files")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
