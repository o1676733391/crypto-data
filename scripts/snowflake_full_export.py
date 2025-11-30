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
from src.snowflake_data_exporter import SnowflakeDataExporter


def parse_args(argv: List[str] | None = None):
    parser = argparse.ArgumentParser(description="Export full Snowflake database/schema: DDL + optionally data")
    parser.add_argument("--database", help="Snowflake database (overrides config)")
    parser.add_argument("--schema", help="Snowflake schema (overrides config)")
    parser.add_argument("--output-dir", default="snowflake_export", help="Directory to write export files")
    parser.add_argument("--no-data", dest="data", action="store_false", help="Do not export table data, only schema (default: export data)")
    parser.add_argument("--compress", dest="compress", action="store_true", help="Compress table exports as .gz (default True)")
    parser.add_argument("--chunk-size", type=int, default=10000, help="Number of rows to fetch per batch when exporting tables")
    return parser.parse_args(argv)


def main(argv: List[str] | None = None) -> int:
    args = parse_args(argv)
    outdir = Path(args.output_dir)
    outdir.mkdir(parents=True, exist_ok=True)

    print("Starting full export")
    schema_exporter = SnowflakeSchemaExporter(database=args.database, schema=args.schema)
    print("Exporting DDLs...")
    exported = schema_exporter.export_schema(str(outdir))
    total_ddl = sum(len(v) for v in exported.values())
    print(f"Exported {total_ddl} DDL files into '{outdir}'")

    if args.data:
        print("Exporting table data (streaming)")
        data_exporter = SnowflakeDataExporter(database=args.database, schema=args.schema, chunk_size=args.chunk_size)
        files = data_exporter.export_all_tables(str(outdir), compress=True)
        print(f"Exported {len(files)} tables to '{outdir / 'data'}'")

    print("Full export complete")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
