import datetime
import os
from contextlib import contextmanager
from unittest.mock import Mock

import psycopg
import pytest
from psycopg import sql

from adscrawler.dbcon.queries import clean_app_ranks_weekly_table


def partition(date, name=None, schema="frontend"):
    return schema, name or f"store_app_ranks_{date:%Y_%m_%d}"


def test_retention_detaches_and_drops_unexpected_partitions():
    today = datetime.date(2026, 11, 9)
    conn = Mock()
    cur = Mock()
    cur.fetchone.return_value = (today,)
    cur.fetchall.return_value = [
        partition(today - datetime.timedelta(days=14)),
        partition(today - datetime.timedelta(days=15)),
        partition(today - datetime.timedelta(days=21)),
        partition(today - datetime.timedelta(days=200)),
        partition(today - datetime.timedelta(days=196)),
        partition(today - datetime.timedelta(days=203)),
        partition(today + datetime.timedelta(days=1)),
        partition(today, name="unexpected", schema="other_schema"),
    ]

    @contextmanager
    def connection():
        yield conn, cur

    db = Mock()
    db.get_driver_connection.side_effect = connection
    clean_app_ranks_weekly_table(db)
    commands = [str(call.args[0]) for call in cur.execute.call_args_list]
    detaches = [statement for statement in commands if "DETACH PARTITION" in statement]
    drops = [statement for statement in commands if "DROP TABLE" in statement]
    assert len(detaches) == len(drops) == 5
    for name in ["2026_10_25", "2026_04_23", "2026_04_20", "2026_11_10", "unexpected"]:
        assert any(
            name in statement and "CONCURRENTLY" in statement for statement in detaches
        )
        assert any(name in statement for statement in drops)
    assert not any("LOCK TABLE" in statement for statement in commands)
    conn.commit.assert_called_once()
    assert conn.autocommit is False


def test_detach_failure_restores_connection_autocommit():
    conn, cur = Mock(), Mock()
    cur.fetchone.return_value = (datetime.date(2026, 11, 9),)
    cur.fetchall.return_value = [("frontend", "unexpected")]

    def execute(statement):
        if "DETACH PARTITION" in str(statement):
            raise RuntimeError("detach failed")

    cur.execute.side_effect = execute

    @contextmanager
    def connection():
        yield conn, cur

    db = Mock()
    db.get_driver_connection.side_effect = connection
    with pytest.raises(RuntimeError, match="detach failed"):
        clean_app_ranks_weekly_table(db)
    assert conn.autocommit is False
    assert not any(
        "DROP TABLE" in str(call.args[0]) for call in cur.execute.call_args_list
    )


@pytest.mark.skipif(
    not os.environ.get("TEST_POSTGRES_DSN"), reason="PostgreSQL test DSN not set"
)
def test_retention_drops_actual_postgres_partitions():
    dsn = os.environ["TEST_POSTGRES_DSN"]

    class RealDB:
        @contextmanager
        def get_driver_connection(self):
            with psycopg.connect(dsn) as conn, conn.cursor() as cur:
                yield conn, cur

    with psycopg.connect(dsn, autocommit=True) as conn:
        assert not conn.execute(
            "SELECT 1 FROM pg_namespace WHERE nspname = 'frontend'"
        ).fetchall()
        conn.execute("CREATE SCHEMA frontend")
        try:
            today = conn.execute("SELECT CURRENT_DATE").fetchone()[0]
            monday = today - datetime.timedelta(days=today.weekday())
            old_monday = monday - datetime.timedelta(days=203)
            dates = [
                today,
                today - datetime.timedelta(days=14),
                today - datetime.timedelta(days=15),
                monday - datetime.timedelta(days=21),
                old_monday,
                today + datetime.timedelta(days=1),
            ]
            conn.execute(
                "CREATE TABLE frontend.app_ranks_weekly (crawled_date date NOT NULL) PARTITION BY RANGE (crawled_date)"
            )
            for date in dates:
                conn.execute(
                    sql.SQL(
                        "CREATE TABLE frontend.{} PARTITION OF frontend.app_ranks_weekly FOR VALUES FROM ({}) TO ({})"
                    ).format(
                        sql.Identifier(f"store_app_ranks_{date:%Y_%m_%d}"),
                        sql.Literal(date),
                        sql.Literal(date + datetime.timedelta(days=1)),
                    )
                )
                conn.execute(
                    "INSERT INTO frontend.app_ranks_weekly VALUES (%s)", (date,)
                )
            clean_app_ranks_weekly_table(RealDB())
            remaining = {
                row[0]
                for row in conn.execute(
                    "SELECT crawled_date FROM frontend.app_ranks_weekly"
                )
            }
            expected = {
                date
                for date in dates
                if today - datetime.timedelta(days=200) <= date <= today
                and (date >= today - datetime.timedelta(days=14) or date.weekday() == 0)
            }
            assert remaining == expected
        finally:
            conn.execute("DROP SCHEMA frontend CASCADE")
