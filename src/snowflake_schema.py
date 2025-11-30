from __future__ import annotations

import os
from typing import Dict, List, Optional

import snowflake.connector

from .config import get_settings

# Optional: support private-key authentication (for accounts with MFA)
try:
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.backends import default_backend
except Exception:  # pragma: no cover - optional dependency
    serialization = None
    default_backend = None


class SnowflakeSchemaExporter:
    def __init__(self, database: Optional[str] = None, schema: Optional[str] = None):
        settings = get_settings()
        self._settings = settings
        self.database = database or settings.snowflake_database
        self.schema = schema or settings.snowflake_schema
        self._ctx = None

    def _get_connection(self):
        if self._ctx is None:
            # If a private key path is provided via env, use key-pair auth (works around MFA)
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

    def _exec(self, sql: str, params: Optional[Dict] = None):
        cur = self._get_connection().cursor()
        try:
            cur.execute(sql, params or {})
            return cur.fetchall()
        finally:
            cur.close()

    def _show(self, show_cmd: str) -> List[Dict]:
        cur = self._get_connection().cursor(snowflake.connector.DictCursor)
        try:
            cur.execute(show_cmd)
            return list(cur)
        finally:
            cur.close()

    def list_tables(self) -> List[str]:
        rows = self._show(f"SHOW TABLES IN SCHEMA {self.database}.{self.schema}")
        return [r["name"] for r in rows]

    def list_views(self) -> List[str]:
        rows = self._show(f"SHOW VIEWS IN SCHEMA {self.database}.{self.schema}")
        return [r["name"] for r in rows]

    def list_materialized_views(self) -> List[str]:
        try:
            rows = self._show(f"SHOW MATERIALIZED VIEWS IN SCHEMA {self.database}.{self.schema}")
            return [r["name"] for r in rows]
        except Exception:
            return []

    def list_sequences(self) -> List[str]:
        try:
            rows = self._show(f"SHOW SEQUENCES IN SCHEMA {self.database}.{self.schema}")
            return [r["name"] for r in rows]
        except Exception:
            return []

    def list_file_formats(self) -> List[str]:
        try:
            rows = self._show(f"SHOW FILE FORMATS IN SCHEMA {self.database}.{self.schema}")
            return [r["name"] for r in rows]
        except Exception:
            return []

    def get_ddl(self, object_type: str, name: str) -> str:
        # object_type should be like TABLE, VIEW, SEQUENCE, FILE FORMAT, etc.
        full_name = f"{self.database}.{self.schema}.{name}"
        cur = self._get_connection().cursor()
        try:
            cur.execute(f"SELECT GET_DDL('{object_type}', %s)", (full_name,))
            row = cur.fetchone()
            return row[0] if row and row[0] else ""
        finally:
            cur.close()

    def export_schema(self, output_dir: str, object_types: Optional[List[str]] = None) -> Dict[str, List[str]]:
        os.makedirs(output_dir, exist_ok=True)
        exported: Dict[str, List[str]] = {}

        types = object_types or ["tables", "views", "materialized_views", "sequences", "file_formats"]

        if "tables" in types:
            names = self.list_tables()
            exported["tables"] = []
            for n in names:
                ddl = self.get_ddl("TABLE", n)
                if ddl:
                    fname = os.path.join(output_dir, f"table__{n}.sql")
                    with open(fname, "w", encoding="utf-8") as f:
                        f.write(ddl)
                    exported["tables"].append(fname)

        if "views" in types:
            names = self.list_views()
            exported["views"] = []
            for n in names:
                ddl = self.get_ddl("VIEW", n)
                if ddl:
                    fname = os.path.join(output_dir, f"view__{n}.sql")
                    with open(fname, "w", encoding="utf-8") as f:
                        f.write(ddl)
                    exported["views"].append(fname)

        if "materialized_views" in types:
            names = self.list_materialized_views()
            exported["materialized_views"] = []
            for n in names:
                ddl = self.get_ddl("MATERIALIZED VIEW", n)
                if ddl:
                    fname = os.path.join(output_dir, f"matview__{n}.sql")
                    with open(fname, "w", encoding="utf-8") as f:
                        f.write(ddl)
                    exported["materialized_views"].append(fname)

        if "sequences" in types:
            names = self.list_sequences()
            exported["sequences"] = []
            for n in names:
                ddl = self.get_ddl("SEQUENCE", n)
                if ddl:
                    fname = os.path.join(output_dir, f"sequence__{n}.sql")
                    with open(fname, "w", encoding="utf-8") as f:
                        f.write(ddl)
                    exported["sequences"].append(fname)

        if "file_formats" in types:
            names = self.list_file_formats()
            exported["file_formats"] = []
            for n in names:
                ddl = self.get_ddl("FILE FORMAT", n)
                if ddl:
                    fname = os.path.join(output_dir, f"fileformat__{n}.sql")
                    with open(fname, "w", encoding="utf-8") as f:
                        f.write(ddl)
                    exported["file_formats"].append(fname)

        return exported


__all__ = ["SnowflakeSchemaExporter"]
