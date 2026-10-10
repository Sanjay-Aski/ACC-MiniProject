import pytest
import psycopg
from psycopg.conninfo import conninfo_to_dict
from psycopg import sql
from app import db


@pytest.fixture(scope="session", autouse=True)
def isolated_database():
    settings = conninfo_to_dict(db.DATABASE_URL)
    # Integration tests never remove application logs or publish into its queues.
    if settings.get("dbname") != "dedup_tests":
        pytest.skip("Run integration tests through the Compose test profile (isolated database required)")
    settings["dbname"] = "postgres"
    with psycopg.connect(**settings, autocommit=True) as conn:
        exists = conn.execute("SELECT 1 FROM pg_database WHERE datname='dedup_tests'").fetchone()
        if not exists:
            conn.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier("dedup_tests")))
    db.initialize()
