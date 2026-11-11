import datetime
import os
from contextlib import contextmanager
from unittest.mock import Mock

import psycopg
import pytest
from psycopg import sql

from adscrawler.dbcon.queries import clean_app_metrics_history_table

TABLE = "app_country_metrics_history"
TODAY = datetime.date(2026, 11, 9)
CUTOFF = TODAY - datetime.timedelta(days=400)


def partition(week, *, name=None, bound=None, schema="public", pending=False):
    return (
        schema,
        name or f"{TABLE}_{week:%Y_%m_%d}",
        bound
        or f"FOR VALUES FROM ('{week}') TO ('{week + datetime.timedelta(days=1)}')",
        pending,
    )


def fake_db(partitions):
    conn, cur = Mock(), Mock()
    cur.fetchone.return_value = (TODAY,)
    cur.fetchall.return_value = partitions

    @contextmanager
    def connection():
        yield conn, cur

    return Mock(get_driver_connection=connection), conn, cur


def test_only_valid_expired_partitions_are_dropped():
    old = CUTOFF - datetime.timedelta(days=1)
    db, conn, cur = fake_db(
        [
            partition(old),
            partition(CUTOFF),
            partition(TODAY),
            partition(old, name="other"),
            partition(old, name=f"{TABLE}_2020_99_99"),
            partition(old, bound="DEFAULT"),
            partition(
                old,
                bound=f"FOR VALUES FROM ('{old}') TO ('{old + datetime.timedelta(days=7)}')",
            ),
        ]
    )
    clean_app_metrics_history_table(db, TABLE)
    commands = [str(c.args[0]) for c in cur.execute.call_args_list]
    detaches = [c for c in commands if "DETACH PARTITION" in c]
    drops = [c for c in commands if "DROP TABLE" in c]
    assert len(detaches) == len(drops) == 1
    assert old.strftime("%Y_%m_%d") in detaches[0]
    assert "CONCURRENTLY" in detaches[0] and old.strftime("%Y_%m_%d") in drops[0]
    assert conn.autocommit is False


def test_rejects_invalid_table():
    db = Mock()
    with pytest.raises(ValueError, match="Invalid metrics history table"):
        clean_app_metrics_history_table(db, "other")
    db.get_driver_connection.assert_not_called()


def test_global_table_and_other_schema_partition():
    table = "app_global_metrics_history"
    old = CUTOFF - datetime.timedelta(days=1)
    name = f"{table}_{old:%Y_%m_%d}"
    db, _, cur = fake_db([partition(old, name=name, schema="archive")])
    clean_app_metrics_history_table(db, table)
    commands = [str(c.args[0]) for c in cur.execute.call_args_list]
    assert any(
        "DETACH PARTITION" in c and "archive" in c and name in c for c in commands
    )
    assert any("DROP TABLE" in c and "archive" in c and name in c for c in commands)


def test_pending_detach_is_not_dropped():
    old = CUTOFF - datetime.timedelta(days=1)
    db, conn, cur = fake_db(
        [partition(old - datetime.timedelta(days=1)), partition(old, pending=True)]
    )
    with pytest.raises(RuntimeError, match="pending detach"):
        clean_app_metrics_history_table(db, TABLE)
    assert conn.autocommit is False
    assert not any(
        "DETACH PARTITION" in str(c.args[0]) or "DROP TABLE" in str(c.args[0])
        for c in cur.execute.call_args_list
    )


def test_detach_failure_does_not_drop():
    old = CUTOFF - datetime.timedelta(days=1)
    db, conn, cur = fake_db([partition(old)])

    def execute(statement, *args):
        if "DETACH PARTITION" in str(statement):
            raise RuntimeError("detach failed")

    cur.execute.side_effect = execute
    with pytest.raises(RuntimeError, match="detach failed"):
        clean_app_metrics_history_table(db, TABLE)
    assert conn.autocommit is False
    assert not any("DROP TABLE" in str(c.args[0]) for c in cur.execute.call_args_list)


@pytest.mark.skipif(
    not os.environ.get("TEST_POSTGRES_DSN"), reason="PostgreSQL test DSN not set"
)
def test_postgres_metrics_retention():
    dsn = os.environ["TEST_POSTGRES_DSN"]

    class RealDB:
        @contextmanager
        def get_driver_connection(self):
            with psycopg.connect(dsn) as conn, conn.cursor() as cur:
                yield conn, cur

    with psycopg.connect(dsn, autocommit=True) as conn:
        assert (
            conn.execute("SELECT to_regclass(%s)", (f"public.{TABLE}",)).fetchone()[0]
            is None
        )
        today = conn.execute("SELECT CURRENT_DATE").fetchone()[0]
        cutoff = today - datetime.timedelta(days=400)
        dates = (
            cutoff - datetime.timedelta(days=1),
            cutoff,
            cutoff + datetime.timedelta(days=1),
        )
        parent = sql.Identifier("public", TABLE)
        created = False
        try:
            conn.execute(
                sql.SQL(
                    "CREATE TABLE {} (week_start date NOT NULL) PARTITION BY RANGE (week_start)"
                ).format(parent)
            )
            created = True
            for week in dates:
                child = sql.Identifier("public", f"{TABLE}_{week:%Y_%m_%d}")
                conn.execute(
                    sql.SQL(
                        "CREATE TABLE {} PARTITION OF {} FOR VALUES FROM ({}) TO ({})"
                    ).format(
                        child,
                        parent,
                        sql.Literal(week),
                        sql.Literal(week + datetime.timedelta(days=1)),
                    )
                )
                conn.execute(
                    sql.SQL("INSERT INTO {} VALUES (%s)").format(parent), (week,)
                )
            clean_app_metrics_history_table(RealDB(), TABLE)
            assert conn.execute(
                sql.SQL("SELECT week_start FROM {} ORDER BY week_start").format(parent)
            ).fetchall() == [(cutoff,), (cutoff + datetime.timedelta(days=1),)]
            assert (
                conn.execute(
                    "SELECT to_regclass(%s)", (f"public.{TABLE}_{dates[0]:%Y_%m_%d}",)
                ).fetchone()[0]
                is None
            )
        finally:
            if created:
                conn.execute(sql.SQL("DROP TABLE {} CASCADE").format(parent))
