from __future__ import annotations

import csv
import gzip
import json
import os
from typing import Iterable, List, Optional

import snowflake.connector

from .config import get_settings
from .snowflake_schema import SnowflakeSchemaExporter

# Optional: support private-key authentication (for accounts with MFA)
try:
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.backends import default_backend
except Exception:  # pragma: no cover - optional dependency
    serialization = None
    default_backend = None


class SnowflakeDataExporter:
    def __init__(self, database: Optional[str] = None, schema: Optional[str] = None, chunk_size: int = 10000):
        settings = get_settings()
        self._settings = settings
        self.database = database or settings.snowflake_database
        self.schema = schema or settings.snowflake_schema
        self.chunk_size = chunk_size
        self._ctx = None
        self._schema_exporter = SnowflakeSchemaExporter(database=self.database, schema=self.schema)

    def _get_connection(self):
        if self._ctx is None:
            pk_path = os.environ.get("SNOWFLAKE_PRIVATE_KEY_PATH")
            pk_pass = os.environ.get("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE")
            connect_kwargs = dict(
                account=self._settings.snowflake_account,
                user=self._settings.snowflake_user,
                warehouse=self._settings.snowflake_warehouse,
                database=self.database,
                schema=self.schema,
                role=self._settings.snowflake_role,
            )

            if pk_path:
                if serialization is None:
                    raise RuntimeError("cryptography library is required for private-key auth. Install 'cryptography'.")
                with open(pk_path, "rb") as f:
                    key_data = f.read()
                password_bytes = pk_pass.encode() if pk_pass else None
                pkey = serialization.load_pem_private_key(key_data, password=password_bytes, backend=default_backend())
                private_key_bytes = pkey.private_bytes(
                    encoding=serialization.Encoding.DER,
                    format=serialization.PrivateFormat.PKCS8,
                    encryption_algorithm=serialization.NoEncryption(),
                )
                connect_kwargs["private_key"] = private_key_bytes
            else:
                connect_kwargs["password"] = self._settings.snowflake_password

            self._ctx = snowflake.connector.connect(**connect_kwargs)
        return self._ctx

    def _row_to_serializable(self, row: dict, columns: List[str]) -> List[str]:
        out: List[str] = []
        for c in columns:
            v = row.get(c)
            if v is None:
                out.append("")
            elif isinstance(v, (dict, list)):
                out.append(json.dumps(v, ensure_ascii=False))
            else:
                try:
                    out.append(str(v))
                except Exception:
                    out.append(json.dumps(v, ensure_ascii=False))
        return out

    def export_table_to_csv(self, table_name: str, output_path: str, compress: bool = True) -> str:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        conn = self._get_connection()
        cur = conn.cursor(snowflake.connector.DictCursor)
        full_table = f"{self.database}.{self.schema}.{table_name}"
        cur.execute(f"SELECT * FROM {full_table}")

        columns = [d[0] for d in cur.description]

        if compress:
            fobj = gzip.open(output_path, "wt", encoding="utf-8")
        else:
            fobj = open(output_path, "w", encoding="utf-8", newline="")

        writer = csv.writer(fobj)
        writer.writerow(columns)

        try:
            while True:
                rows = cur.fetchmany(self.chunk_size)
                if not rows:
                    break
                for row in rows:
                    writer.writerow(self._row_to_serializable(row, columns))
        finally:
            cur.close()
            fobj.close()

        return output_path

    def export_all_tables(self, output_dir: str, compress: bool = True) -> List[str]:
        data_dir = os.path.join(output_dir, "data")
        os.makedirs(data_dir, exist_ok=True)
        exported_files: List[str] = []
        tables = self._schema_exporter.list_tables()
        for t in tables:
            safe_name = t.replace("/", "_")
            filename = f"table__{safe_name}.csv.gz" if compress else f"table__{safe_name}.csv"
            outpath = os.path.join(data_dir, filename)
            print(f"Exporting table {t} -> {outpath}")
            try:
                self.export_table_to_csv(t, outpath, compress=compress)
                exported_files.append(outpath)
            except Exception as e:
                print(f"Error exporting table {t}: {e}")
        return exported_files


__all__ = ["SnowflakeDataExporter"]
