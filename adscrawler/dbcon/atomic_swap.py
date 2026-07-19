"""Atomic partition swaps with optimized transaction isolation."""

import datetime
import io
from typing import Any

import pandas as pd
from psycopg import sql

from adscrawler.config import get_logger
from adscrawler.dbcon.connection import PostgresEngine

logger = get_logger(__name__)


def _apply_parent_indexes_to_staging(
    cursor: Any, schema: str, parent_table: str, staging_table: str, date_str: str
) -> None:
    """Find all indexes on the parent table and replicate them dynamically on the staging table."""
    # Query to fetch definitions of all indexes belonging to the parent table
    cursor.execute(
        """
        SELECT c.relname, pg_get_indexdef(i.indexrelid)
        FROM pg_index i
        JOIN pg_class c ON c.oid = i.indexrelid
        JOIN pg_class p ON p.oid = i.indrelid
        JOIN pg_namespace n ON n.oid = p.relnamespace
        WHERE n.nspname = %(schema)s AND p.relname = %(parent_table)s;
        """,
        {"schema": schema, "parent_table": parent_table},
    )
    parent_indexes = cursor.fetchall()

    for old_index_name, index_def in parent_indexes:
        # Example index_def:
        # CREATE INDEX idx_changes_domain_id ON adtech.domain_app_changes USING btree (domain_id)

        # 1. Generate a completely unique name for the new staging index to avoid collisions
        new_index_name = f"idx_{staging_table}_{old_index_name}_{date_str}"

        # 2. Safely swap the target table name and the index name in the raw SQL definition string
        # We replace the parent table name with the staging table name
        localized_def = index_def.replace(
            f"{schema}.{parent_table}", f"{schema}.{staging_table}"
        ).replace(f" {parent_table} ", f" {staging_table} ")

        # Replace the old index name with our unique staging index name
        localized_def = localized_def.replace(old_index_name, new_index_name)

        logger.info("Dynamically creating replicated index: %s", new_index_name)
        cursor.execute(sql.SQL(localized_def))


def _get_existing_partitions(cursor: Any, schema: str, table: str) -> list[str]:
    """Return the names of all currently ATTACHED child partitions."""
    cursor.execute(
        """
        SELECT c.relname
        FROM pg_inherits i
        JOIN pg_class c ON c.oid = i.inhrelid
        JOIN pg_class p ON p.oid = i.inhparent
        JOIN pg_namespace n ON n.oid = p.relnamespace
        WHERE n.nspname = %(schema)s AND p.relname = %(table)s
        """,
        {"schema": schema, "table": table},
    )
    return [row[0] for row in cursor.fetchall()]


def atomic_swap_partition(
    df: pd.DataFrame,
    pgdb: PostgresEngine,
    schema: str = "adtech",
    table: str = "domain_app_changes",
    batch_date: datetime.date | None = None,
) -> None:
    """Swap data for *batch_date* into a list-partitioned table with zero downtime."""
    if batch_date is None:
        batch_date = df["batch_date"].iloc[0]
        if isinstance(batch_date, pd.Timestamp):
            batch_date = batch_date.date()

    date_str = batch_date.strftime("%Y%m%d")
    staging_table_name = f"{table}_{date_str}"

    id_schema = sql.Identifier(schema)
    id_parent = sql.Identifier(table)
    id_staging = sql.Identifier(schema, staging_table_name)
    id_constraint = sql.Identifier(f"{table}_{date_str}_batch_date_check")

    with pgdb.get_driver_connection(autocommit=False) as (conn, cur):
        logger.info(
            "Preparing standalone staging table: %s.%s", schema, staging_table_name
        )

        with conn.transaction():
            cur.execute(sql.SQL("DROP TABLE IF EXISTS {};").format(id_staging))
            cur.execute(
                sql.SQL("""
                    CREATE TABLE {} (
                        LIKE {}.{} INCLUDING DEFAULTS INCLUDING STORAGE
                    );
                """).format(id_staging, id_schema, id_parent)
            )

            cur.execute(
                sql.SQL("""
                    ALTER TABLE {} ADD CONSTRAINT {} 
                    CHECK (batch_date = {})
                """).format(id_staging, id_constraint, sql.Literal(batch_date))
            )

            logger.info("Bulk copying %s rows with FREEZE optimization...", len(df))
            _copy_df_to_table_freeze(df, id_staging, cur)

        logger.info("Analyzing parent table index structure to mirror layout...")
        _apply_parent_indexes_to_staging(
            cursor=cur,
            schema=schema,
            parent_table=table,
            staging_table=staging_table_name,
            date_str=date_str,
        )

        conn.commit()
        cur.execute(sql.SQL("ANALYZE {};").format(id_staging))
        conn.commit()

        attached_partitions = _get_existing_partitions(cur, schema, table)
        logger.info(
            "Active child partitions slated for detachment: %s", attached_partitions
        )

        with conn.transaction():
            for old_partition in attached_partitions:
                cur.execute(
                    sql.SQL("""
                            ALTER TABLE {}.{} DETACH PARTITION {}.{};
                        """).format(
                        id_schema,
                        id_parent,
                        id_schema,
                        sql.Identifier(old_partition),
                    )
                )

            cur.execute(
                sql.SQL("""
                        ALTER TABLE {}.{} ATTACH PARTITION {} 
                        FOR VALUES IN ({})
                    """).format(
                    id_schema, id_parent, id_staging, sql.Literal(batch_date)
                )
            )

            logger.info("Cutover transaction committed successfully.")

        for old_partition in attached_partitions:
            if old_partition != staging_table_name:
                logger.info("Pruning historical detached partition: %s", old_partition)
                cur.execute(
                    sql.SQL("DROP TABLE IF EXISTS {}.{};").format(
                        id_schema, sql.Identifier(old_partition)
                    )
                )
    logger.info("Pipeline complete for batch_date=%s", batch_date)


def _copy_df_to_table_freeze(
    df: pd.DataFrame, id_staging: sql.Identifier, cursor: Any
) -> None:
    columns = [sql.Identifier(col) for col in df.columns]
    col_list = sql.SQL(", ").join(columns)

    buffer = io.StringIO()
    df.to_csv(buffer, index=False, header=False, sep="\t", na_rep="\\N")
    buffer.seek(0)

    copy_query = sql.SQL("COPY {} ({}) FROM STDIN WITH (FREEZE, FORMAT text)").format(
        id_staging, col_list
    )

    with cursor.copy(copy_query) as copy:
        while data := buffer.read(1048576):
            copy.write(data)
