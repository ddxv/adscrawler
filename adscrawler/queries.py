import pandas as pd
import datetime
from adscrawler.connection import PostgresCon
from adscrawler.config import get_logger
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


def query_developers(database_connection, store, limit: int = 1000) -> pd.DataFrame:
    before_date = (datetime.datetime.today() - datetime.timedelta(days=7)).strftime(
        "%Y-%m-%d"
    )
    sel_query = f"""SELECT
        *
        FROM
        developers d
        LEFT JOIN logging.developers_crawled_at dc
        ON d.id = dc.developer
        WHERE store = {store} 
            AND (apps_crawled_at <= '{before_date}' OR apps_crawled_at IS NULL)
        ORDER BY apps_crawled_at NULLS FIRST
        limit {limit}
        ;
        """
    df = pd.read_sql(sel_query, database_connection.engine)
    return df


def query_store_ids(database_connection, store: int) -> list:
    sel_query = f"""SELECT
        store_id
        FROM
        store_apps
        WHERE store = {store}
        ;
        """
    df = pd.read_sql(sel_query, database_connection.engine)
    store_ids = df["store_id"].tolist()
    return store_ids


def query_pub_domains(database_connection, limit=10000) -> pd.DataFrame:
    """Query pub domains
    that have apps which are ad supported and still on store
    params: limit: int number of rows to return
    """
    sel_query = f"""SELECT
            pd.id, pd.url, pd.crawled_at
        FROM
            app_urls_map aum
        LEFT JOIN pub_domains pd ON
            pd.id = aum.pub_domain
        LEFT JOIN store_apps sa ON
            sa.id = aum.store_app
        WHERE
            sa.ad_supported
            AND sa.crawl_result = 1
        ORDER BY
            pd.crawled_at NULLS FIRST
        LIMIT {limit}
        ; 
        """
    df = pd.read_sql(sel_query, database_connection.engine)
    return df


def delete_app_url_mapping(store_app: int, database_connection: PostgresCon) -> None:
    del_query = f"""DELETE
        FROM app_urls_map
        WHERE store_app = {store_app}
        ;
        """
    with database_connection.engine.begin() as conn:
        conn.exec_driver_sql(del_query)


def query_store_apps(
    stores: list[int], database_connection: PostgresCon, limit: int = 1000
) -> pd.DataFrame:
    before_date = (datetime.datetime.today() - datetime.timedelta(days=3)).strftime(
        "%Y-%m-%d"
    )
    where_str = "store IN (" + (", ").join([str(x) for x in stores]) + ")"
    where_str += """ AND (installs >= 100
                    OR review_count >= 10
                    OR crawl_result IS NULL) """
    where_str += f""" AND updated_at <= '{before_date}'"""
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
