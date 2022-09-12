import pandas as pd
from dbcon.connection import PostgresCon
from config import get_logger
import numpy as np
import uuid

logger = get_logger(__name__)


def upsert_df(
    df: pd.DataFrame,
    table_name: str,
    database_connection: PostgresCon,
    key_columns: list[str],
    insert_columns: list[str],
    return_rows: bool = False,
    schema: str = None,
    log: bool = False,
) -> None | pd.DataFrame:
    """
    Perform an "upsert" on a PostgreSQL table from a DataFrame.
    Constructs an INSERT … ON CONFLICT statement, uploads the DataFrame to a
    temporary table, and then executes the INSERT.
    Parameters
    ----------
    data_frame : pandas.DataFrame
        The DataFrame to be upserted.
    table_name : str
        The name of the target table.
    engine : sqlalchemy.engine.Engine
        The SQLAlchemy Engine to use.
    schema : str, optional
        The name of the schema containing the target table.
    key_columns : list of str, optional
        A list of the column name(s) on which to match. If omitted, the
        primary key columns of the target table will be used.
    """
    table_spec = ""
    if schema:
        table_spec += '"' + schema.replace('"', '""') + '".'
    table_spec += '"' + table_name.replace('"', '""') + '"'

    all_columns = list(set(key_columns + insert_columns))

    insert_col_list = ", ".join([f'"{col_name}"' for col_name in all_columns])
    match_col_list = ", ".join([f'"{col}"' for col in key_columns])
    update_on = ", ".join([f'"{col}" = EXCLUDED."{col}"' for col in all_columns])

    if return_rows:
        returning_str = " RETURNING * ;"
    else:
        returning_str = ""

    temp_table = f"temp_{uuid.uuid4().hex[:6]}"

    sql_query = f"""INSERT INTO {table_spec} ({insert_col_list})
                SELECT {insert_col_list} FROM {temp_table}
                ON CONFLICT ({match_col_list}) 
                DO UPDATE SET
                    {update_on}
                {returning_str}
    """

    if log:
        logger.info(sql_query)

    with database_connection.engine.begin() as conn:
        conn.exec_driver_sql(f"DROP TABLE IF EXISTS {temp_table}")
        conn.exec_driver_sql(
            f"""CREATE TEMPORARY TABLE {temp_table} 
            AS SELECT * FROM {table_spec} WHERE false"""
        )
        df[all_columns].to_sql(
            temp_table,
            con=conn,
            if_exists="append",
            index=False,
        )
        result = conn.exec_driver_sql(sql_query)
        if return_rows:
            if result.returns_rows:
                get_df = pd.DataFrame(result.mappings().all())
            else:
                logger.warning("Sqlalchemy result did not have rows")
                get_df = pd.DataFrame()
        conn.execute(f'DROP TABLE "{temp_table}"')
    if return_rows:
        return get_df


def query_pub_domains(database_connection, limit=10000):
    # Query Pub Domain Table
    sel_query = f"""SELECT id, url, crawled_at
        FROM pub_domains
        ORDER BY crawled_at NULLS FIRST
        limit {limit}
        """
    df = pd.read_sql(sel_query, database_connection.engine)
    return df


def query_store_apps(
    stores: list[int], database_connection, limit: int = 1000
) -> pd.DataFrame:
    where_str = "store IN (" + (", ").join([str(x) for x in stores]) + ")"
    where_str += """ AND (installs >= 10000 
                    OR review_count >= 1000
                    OR crawl_result IS NULL) """
    sel_query = f"""SELECT store, id as store_app, store_id, updated_at  
        FROM store_apps
        WHERE {where_str}
        ORDER BY
            (CASE
                WHEN crawl_result IS NULL THEN 0
                ELSE 1
            END),
            updated_at
        limit {limit}
        """
    df = pd.read_sql(sel_query, database_connection.engine)
    return df


def query_all(
    table_name: str, key_cols: list[str] | str, df: pd.DataFrame, database_connection
) -> pd.DataFrame:
    if isinstance(key_cols, str):
        key_cols = [key_cols]
    wheres = []
    for key_col in key_cols:
        keys = df[key_col].unique().tolist()
        if all([isinstance(x, (np.integer, int)) for x in keys]):
            values_str = "(" + (", ").join([str(x) for x in keys]) + ")"
        else:
            values_str = "('" + ("', '").join(keys) + "')"
            values_str = values_str.replace("%", "%%")
        where = f"{key_col} IN {values_str}"
        wheres.append(where)
    where_str = " AND ".join(wheres)
    sel_query = f"""SELECT *
    FROM {table_name}
    WHERE {where_str}
    """
    # logger.info(sel_query)
    df = pd.read_sql(sel_query, database_connection.engine)
    return df
