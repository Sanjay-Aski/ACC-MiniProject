#!/usr/bin/env python3
"""Apply schema.sql and grant the Function App identity least-privilege table access.
Requires mssql-python and a Microsoft Entra admin session.
"""
from __future__ import annotations
import argparse
import os
import re
from pathlib import Path

import mssql_python

ROOT = Path(__file__).resolve().parents[1]


def connect(server: str, database: str):
    # ActiveDirectoryDefault uses the current Azure CLI login during deployment.
    return mssql_python.connect(
        server=server,
        database=database,
        authentication="ActiveDirectoryDefault",
        encrypt="yes",
        trust_server_certificate="no",
        timeout=30,
        autocommit=True,
    )


def split_go(script: str) -> list[str]:
    return [part.strip() for part in re.split(r"(?im)^\s*GO\s*(?:--.*)?$", script) if part.strip()]


def bracket(name: str) -> str:
    return "[" + name.replace("]", "]]" ) + "]"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--server", default=os.getenv("SQL_SERVER"), help="SQL logical server hostname (without tcp:)")
    parser.add_argument("--database", default=os.getenv("SQL_DATABASE"), help="SQL database name")
    parser.add_argument("--function-app", default=os.getenv("FUNCTION_APP_NAME"), help="Function App system-assigned identity display name")
    parser.add_argument("--schema", default=str(ROOT / "schema.sql"))
    args = parser.parse_args()
    if not args.server or not args.database or not args.function_app:
        parser.error("Provide --server, --database, and --function-app (or matching environment variables).")

    schema = Path(args.schema).read_text(encoding="utf-8")
    try:
        with connect(args.server, args.database) as cn:
            cur = cn.cursor()
            for batch in split_go(schema):
                cur.execute(batch)
            principal = bracket(args.function_app)
            # Azure SQL resolves the Function App's service principal through Entra ID.
            cur.execute(f"IF DATABASE_PRINCIPAL_ID(N'{args.function_app.replace(chr(39), chr(39)*2)}') IS NULL CREATE USER {principal} FROM EXTERNAL PROVIDER;")
            for table, perms in {
                "TestRuns": "SELECT, INSERT, UPDATE",
                "TestMessages": "SELECT, INSERT, UPDATE",
                "FingerprintState": "SELECT, INSERT, UPDATE",
                "AcceptedRecords": "SELECT, INSERT",
                "ProcessingEvents": "SELECT, INSERT",
                "ProcessingFailures": "SELECT, INSERT",
                "DeadLetterCheckpoint": "SELECT, INSERT, UPDATE",
            }.items():
                cur.execute(f"GRANT {perms} ON OBJECT::dbo.{table} TO {principal};")
            cur.execute(f"GRANT EXECUTE ON OBJECT::dbo.ProcessDedupRecord TO {principal};")
            cur.execute(f"GRANT SELECT ON OBJECT::dbo.TestRuns TO {principal};")
        print("Schema applied and Function App SQL permissions granted.")
        return 0
    except Exception as exc:
        print(f"Schema/bootstrap failed: {type(exc).__name__}: {exc}")
        print("Confirm network/firewall access, the signed-in Entra user as SQL admin, and Function App identity visibility.")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
