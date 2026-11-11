import datetime
import os
import uuid
from contextlib import contextmanager, nullcontext
from unittest.mock import Mock

import pandas as pd
import psycopg
import pytest
from psycopg import sql

from adscrawler.dbcon.atomic_swap import atomic_swap_date_partition


class FakeDB:
    def __init__(self, attached=(), fail_attach=False):
        self.conn = Mock()
        self.conn.transaction.side_effect = nullcontext
        self.cur = Mock()
        self.cur.fetchall.return_value = [(name,) for name in attached]
        self.cur.copy.return_value.__enter__ = Mock(return_value=Mock())
        self.cur.copy.return_value.__exit__ = Mock(return_value=False)
        if fail_attach:
            def execute(query, *args):
                if "ATTACH PARTITION" in str(query):
                    raise RuntimeError("attach failed")
            self.cur.execute.side_effect = execute

    @contextmanager
    def get_driver_connection(self, autocommit=False):
        yield self.conn, self.cur


def test_date_swap_only_detaches_target_partition():
    db = FakeDB(["store_app_ranks_2026_04_27", "store_app_ranks_2026_04_28"])
    atomic_swap_date_partition(pd.DataFrame({"crawled_date": [datetime.date(2026, 4, 27)]}), db,
                               "frontend", "app_ranks_weekly", "store_app_ranks", datetime.date(2026, 4, 27))
    statements = [str(call.args[0]) for call in db.cur.execute.call_args_list]
    assert sum("DETACH PARTITION" in s for s in statements) == 1
    assert any("2026_04_27" in s and "DETACH PARTITION" in s for s in statements)
    assert not any("2026_04_28" in s and "DETACH PARTITION" in s for s in statements)
    assert any("FOR VALUES FROM" in s and "2026, 4, 28" in s for s in statements)


def test_date_swap_cleans_staging_after_failure():
    db = FakeDB(["store_app_ranks_2026_04_27"], fail_attach=True)
    with pytest.raises(RuntimeError, match="attach failed"):
        atomic_swap_date_partition(pd.DataFrame({"crawled_date": [datetime.date(2026, 4, 27)]}), db,
                                   "frontend", "app_ranks_weekly", "store_app_ranks", datetime.date(2026, 4, 27))
    db.conn.rollback.assert_called_once()
    assert "DROP TABLE IF EXISTS" in str(db.cur.execute.call_args.args[0])


@pytest.mark.skipif(not os.environ.get("TEST_POSTGRES_DSN"), reason="PostgreSQL test DSN not set")
def test_date_swap_postgres_atomicity():
    dsn = os.environ["TEST_POSTGRES_DSN"]
    schema = f"test_rank_swap_{uuid.uuid4().hex}"
    class RealDB:
        @contextmanager
        def get_driver_connection(self, autocommit=False):
            with psycopg.connect(dsn, autocommit=autocommit) as conn:
                with conn.cursor() as cur:
                    yield conn, cur

    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        try:
            conn.execute(sql.SQL("CREATE TABLE {}.app_ranks_weekly (crawled_date date NOT NULL, rank int NOT NULL) PARTITION BY RANGE (crawled_date)").format(sql.Identifier(schema)))
            for day in (27, 28):
                conn.execute(sql.SQL("CREATE TABLE {}.{} PARTITION OF {}.app_ranks_weekly FOR VALUES FROM ({}) TO ({})").format(
                    sql.Identifier(schema), sql.Identifier(f"store_app_ranks_2026_04_{day}"), sql.Identifier(schema),
                    sql.Literal(datetime.date(2026, 4, day)), sql.Literal(datetime.date(2026, 4, day + 1))))
                conn.execute(sql.SQL("INSERT INTO {}.app_ranks_weekly VALUES (%s, 1)").format(sql.Identifier(schema)), (datetime.date(2026, 4, day),))
            conn.execute(sql.SQL("CREATE INDEX ON {}.app_ranks_weekly (rank)").format(sql.Identifier(schema)))
            db = RealDB()
            date = datetime.date(2026, 4, 27)
            atomic_swap_date_partition(pd.DataFrame({"crawled_date": [date], "rank": [2]}), db, schema, "app_ranks_weekly", "store_app_ranks", date)
            query = sql.SQL("SELECT crawled_date, rank FROM {}.app_ranks_weekly ORDER BY crawled_date").format(sql.Identifier(schema))
            assert conn.execute(query).fetchall() == [(date, 2), (datetime.date(2026, 4, 28), 1)]
            with pytest.raises(psycopg.errors.CheckViolation):
                atomic_swap_date_partition(pd.DataFrame({"crawled_date": [datetime.date(2026, 4, 28)], "rank": [3]}), db, schema, "app_ranks_weekly", "store_app_ranks", date)
            assert conn.execute(query).fetchall() == [(date, 2), (datetime.date(2026, 4, 28), 1)]
            conn.execute(sql.SQL("CREATE UNIQUE INDEX ON {}.app_ranks_weekly (crawled_date, rank)").format(sql.Identifier(schema)))
            with pytest.raises(psycopg.errors.UniqueViolation):
                atomic_swap_date_partition(pd.DataFrame({"crawled_date": [date, date], "rank": [3, 3]}), db, schema, "app_ranks_weekly", "store_app_ranks", date)
            assert conn.execute(query).fetchall() == [(date, 2), (datetime.date(2026, 4, 28), 1)]
            assert not conn.execute("SELECT 1 FROM pg_tables WHERE schemaname = %s AND tablename LIKE 'rank_stage_%%'", (schema,)).fetchall()
        finally:
            conn.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))
