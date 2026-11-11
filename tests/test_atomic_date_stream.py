import datetime
import os
import uuid
from contextlib import contextmanager
from unittest.mock import Mock

import psycopg
import pytest
from psycopg import sql

from adscrawler.dbcon.atomic_swap import atomic_swap_date_partition_stream

DAY = datetime.date(2026, 4, 27)
TARGET = "daily_2026_04_27"


class FakeDB:
    def __init__(self, attached=(), fail=None, autocommit=False):
        self.events = []
        self.fail = fail
        self.conn = Mock()
        self.conn.autocommit = autocommit
        self.conn.closed = False
        self.conn.transaction.side_effect = self.transaction
        self.cur = Mock()
        self.cur.execute.side_effect = self.execute
        self.cur.fetchall.return_value = [(name,) for name in attached]
        self.copy = Mock()
        self.cur.copy.return_value.__enter__ = Mock(return_value=self.copy)
        self.cur.copy.return_value.__exit__ = Mock(return_value=False)

    @contextmanager
    def transaction(self):
        self.events.append("BEGIN")
        try:
            yield
        except Exception:
            self.events.append("ROLLBACK")
            raise
        else:
            self.events.append("COMMIT")

    def execute(self, query, *args):
        statement = str(query)
        self.events.append(statement)
        if self.fail and self.fail in statement:
            raise RuntimeError("injected failure")

    @contextmanager
    def get_driver_connection(self, autocommit=False):
        assert not autocommit
        yield self.conn, self.cur


def swap(db, stream=("2026-04-27\t2\n",), **kwargs):
    atomic_swap_date_partition_stream(
        stream,
        ["week_start", "rank"],
        db,
        "test_schema",
        "parent",
        "daily",
        DAY,
        **kwargs,
    )


def test_streams_and_only_detaches_target_with_ordered_cutover():
    db = FakeDB([TARGET, "daily_2026_04_28"])
    chunks = iter(["2026-04-27\t", "2\n2026-04-27\t\\N\n"])
    swap(db, chunks, unique_keys=["week_start", "rank"])
    assert [call.args[0] for call in db.copy.write.call_args_list] == [
        "2026-04-27\t",
        "2\n2026-04-27\t\\N\n",
    ]
    copy_query = db.cur.copy.call_args.args[0].as_string()
    assert (
        "FREEZE" in copy_query
        and "FORMAT text" in copy_query
        and "NULL '\\N'" in copy_query
    )
    statements = db.events
    create = next(s for s in statements if "CREATE TABLE" in s)
    assert all(
        part in create
        for part in (
            "INCLUDING DEFAULTS",
            "INCLUDING STORAGE",
            "INCLUDING CONSTRAINTS",
            "INCLUDING INDEXES",
        )
    )
    assert any("CREATE UNIQUE INDEX" in s for s in statements)
    assert any("2026, 4, 28" in s and "ADD CHECK" in s for s in statements)
    detach = next(i for i, s in enumerate(statements) if "DETACH PARTITION" in s)
    assert TARGET in statements[detach] and "CONCURRENTLY" in statements[detach]
    assert not any("daily_2026_04_28" in s and "DETACH" in s for s in statements)
    assert next(i for i, s in enumerate(statements) if "ANALYZE" in s) < detach
    drop = next(i for i, s in enumerate(statements) if "DROP TABLE" in s)
    attach = next(i for i, s in enumerate(statements) if "ATTACH PARTITION" in s)
    assert statements[detach + 1] == "BEGIN"
    assert detach < drop < attach and statements[attach + 1] == "COMMIT"
    assert db.conn.autocommit is False


def test_new_partition_and_restore_autocommit():
    db = FakeDB(autocommit=True)
    swap(db, date_column="custom_date")
    assert not any("DETACH PARTITION" in s or "DROP TABLE" in s for s in db.events)
    assert any("custom_date" in s and "ADD CHECK" in s for s in db.events)
    assert db.conn.autocommit is True


def test_attach_failure_rolls_back_old_drop_and_cleans_staging(monkeypatch):
    log = Mock()
    monkeypatch.setattr("adscrawler.dbcon.atomic_swap.logger", log)
    db = FakeDB([TARGET], fail="ATTACH PARTITION")
    with pytest.raises(RuntimeError, match="injected"):
        swap(db)
    assert "remains detached for manual recovery" in log.error.call_args.args[0]
    assert "ROLLBACK" in db.events
    drop = [s for s in db.events if "DROP TABLE" in s]
    assert len(drop) == 2 and TARGET in drop[0] and "rank_stage_" in drop[1]
    assert any("pg_advisory_unlock" in s for s in db.events)
    assert db.conn.autocommit is False


def test_copy_failure_rolls_back_without_dropping_unowned_table():
    db = FakeDB([TARGET])
    db.copy.write.side_effect = RuntimeError("copy failure")
    with pytest.raises(RuntimeError, match="copy failure"):
        swap(db)
    assert "ROLLBACK" in db.events
    assert not any("DROP TABLE" in s or "DETACH PARTITION" in s for s in db.events)
    assert db.conn.autocommit is False


def test_detach_failure_warns_about_pending_and_cleans_staging(monkeypatch):
    log = Mock()
    monkeypatch.setattr("adscrawler.dbcon.atomic_swap.logger", log)
    db = FakeDB([TARGET], fail="DETACH PARTITION")
    with pytest.raises(RuntimeError, match="injected"):
        swap(db)
    assert "inhdetachpending" in log.error.call_args.args[0]
    assert any("DROP TABLE IF EXISTS" in s and "rank_stage_" in s for s in db.events)


def test_invalid_unique_keys_do_not_open_connection():
    with pytest.raises(ValueError, match="unique_keys"):
        swap(FakeDB(), unique_keys=[])


@pytest.mark.skipif(
    not os.environ.get("TEST_POSTGRES_DSN"), reason="PostgreSQL test DSN not set"
)
def test_postgres_stream_swap_and_recovery():
    dsn = os.environ["TEST_POSTGRES_DSN"]
    schema = f"test_stream_swap_{uuid.uuid4().hex}"

    class RealDB:
        @contextmanager
        def get_driver_connection(self, autocommit=False):
            with psycopg.connect(dsn, autocommit=autocommit) as conn:
                with conn.cursor() as cur:
                    yield conn, cur

    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        try:
            conn.execute(
                sql.SQL(
                    "CREATE TABLE {}.parent (week_start date NOT NULL, rank int) PARTITION BY RANGE (week_start)"
                ).format(sql.Identifier(schema))
            )
            conn.execute(
                sql.SQL("CREATE INDEX ON {}.parent (rank)").format(
                    sql.Identifier(schema)
                )
            )
            for day in (27, 28):
                conn.execute(
                    sql.SQL(
                        "CREATE TABLE {}.{} PARTITION OF {}.parent FOR VALUES FROM ({}) TO ({})"
                    ).format(
                        sql.Identifier(schema),
                        sql.Identifier(f"daily_2026_04_{day}"),
                        sql.Identifier(schema),
                        sql.Literal(datetime.date(2026, 4, day)),
                        sql.Literal(datetime.date(2026, 4, day + 1)),
                    )
                )
                conn.execute(
                    sql.SQL("INSERT INTO {}.parent VALUES (%s, 1)").format(
                        sql.Identifier(schema)
                    ),
                    (datetime.date(2026, 4, day),),
                )
            db = RealDB()

            def run(chunks, **kwargs):
                atomic_swap_date_partition_stream(
                    chunks,
                    ["week_start", "rank"],
                    db,
                    schema,
                    "parent",
                    "daily",
                    DAY,
                    **kwargs,
                )

            query = sql.SQL(
                "SELECT week_start, rank FROM {}.parent ORDER BY week_start"
            ).format(sql.Identifier(schema))
            run(
                iter(["2026-04-27\t", "2\n2026-04-27\t\\N\n"]),
                unique_keys=["week_start", "rank"],
            )
            expected = [(DAY, 2), (DAY, None), (datetime.date(2026, 4, 28), 1)]
            assert conn.execute(query).fetchall() == expected
            with pytest.raises(psycopg.errors.CheckViolation):
                run(["2026-04-28\t3\n"])
            with pytest.raises(psycopg.errors.UniqueViolation):
                run(
                    ["2026-04-27\t3\n2026-04-27\t3\n"],
                    unique_keys=["week_start", "rank"],
                )
            assert conn.execute(query).fetchall() == expected
            assert not conn.execute(
                "SELECT 1 FROM pg_tables WHERE schemaname = %s AND tablename LIKE 'rank_stage_%%'",
                (schema,),
            ).fetchall()
        finally:
            conn.execute(
                sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema))
            )
