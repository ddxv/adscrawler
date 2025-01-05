import datetime

import numpy as np
import pandas as pd
from psycopg import Connection
from psycopg.sql import SQL, Composed, Identifier
from sqlalchemy import text

from .config import get_logger
from .connection import PostgresCon

logger = get_logger(__name__)


def upsert_df(
    df: pd.DataFrame,
    table_name: str,
    database_connection: Connection,
    key_columns: list[str],
    insert_columns: list[str],
    return_rows: bool = False,
    schema: str | None = None,
    log: bool = False,
) -> pd.DataFrame | None:
    """Perform an "upsert" on a PostgreSQL table from a DataFrame.
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

    raw_conn = database_connection.engine.raw_connection()

    if "crawled_date" in df.columns and df["crawled_date"].isna().all():
        df["crawled_date"] = pd.to_datetime(df["crawled_date"]).dt.date
        df["crawled_date"] = None
    if "release_date" in df.columns and df["release_date"].isna().all():
        df["release_date"] = None

    all_columns = list(set(key_columns + insert_columns))
    table_identifier = Identifier(table_name)
    if schema:
        table_identifier = Composed([Identifier(schema), SQL("."), table_identifier])

    columns = SQL(", ").join(map(Identifier, all_columns))
    placeholders = SQL(", ").join(SQL("%s") for _ in all_columns)
    conflict_columns = SQL(", ").join(map(Identifier, key_columns))
    update_set = SQL(", ").join(
        SQL("{0} = EXCLUDED.{0}").format(Identifier(col)) for col in all_columns
    )

    # Upsert query without RETURNING clause
    upsert_query = SQL("""
        INSERT INTO {table} ({columns})
        VALUES ({placeholders})
        ON CONFLICT ({conflict_columns})
        DO UPDATE SET {update_set}
    """).format(
        table=table_identifier,
        columns=columns,
        placeholders=placeholders,
        conflict_columns=conflict_columns,
        update_set=update_set,
    )

    sel_where_conditions = SQL(" AND ").join(
        SQL("{} = ANY(%s)").format(Identifier(col)) for col in key_columns
    )

    select_query = SQL("""
        SELECT * FROM {table}
        WHERE {where_conditions}
    """).format(table=table_identifier, where_conditions=sel_where_conditions)
    if log:
        logger.info(f"Upsert query: {upsert_query.as_string(raw_conn)}")
        logger.info(f"Select query: {select_query.as_string(raw_conn)}")

    with raw_conn.cursor() as cur:
        # Perform upsert
        data = [
            tuple(row) for row in df[all_columns].itertuples(index=False, name=None)
        ]
        cur.executemany(upsert_query, data)

        # Fetch affected rows if required
        if return_rows:
            where_values = [df[col].tolist() for col in key_columns]
            cur.execute(select_query, where_values)
            result = cur.fetchall()
            column_names = [desc[0] for desc in cur.description]
            return_df = pd.DataFrame(result, columns=column_names)
        else:
            return_df = None

    raw_conn.commit()
    return return_df


def query_developers(
    database_connection: PostgresCon,
    store: int,
    limit: int = 1000,
) -> pd.DataFrame:
    logger.info(f"Query developers {store=} start")
    before_date = (datetime.datetime.today() - datetime.timedelta(days=15)).strftime(
        "%Y-%m-%d",
    )
    sel_query = f"""SELECT 
            d.*,
            SUM(sa.installs) AS total_installs,
            SUM(sa.review_count) AS total_reviews,
            dc.apps_crawled_at
        FROM
            developers d
        LEFT JOIN logging.developers_crawled_at dc
            ON d.id = dc.developer
        LEFT JOIN store_apps sa 
            ON d.id = sa.developer 
        WHERE d.store = {store} 
            AND (apps_crawled_at <= '{before_date}' OR apps_crawled_at IS NULL)
            AND sa.crawl_result = 1
        GROUP BY
            d.id, dc.apps_crawled_at
        ORDER BY apps_crawled_at::date NULLS FIRST,
        total_installs DESC NULLS LAST,
        total_reviews DESC NULLS LAST
        limit {limit}
        ;
        """
    df = pd.read_sql(sel_query, database_connection.engine)
    logger.info(f"Query developers {store=} returning rows:{df.shape[0]}")
    return df


def query_store_id_map(
    database_connection: PostgresCon,
    store: int | None = None,
    store_ids: list[str] | None = None,
) -> pd.DataFrame:
    where_statement = ""
    if store:
        where_statement += f"WHERE store = {store} "
    if store_ids:
        if "WHERE" not in where_statement:
            where_statement += " WHERE "
        else:
            where_statement += " AND "
        store_ids_list = "'" + "','".join(store_ids) + "'"
        where_statement += f" store_id in ({store_ids_list}) "
    sel_query = f"""SELECT
        id, store, store_id
        FROM
        store_apps
        {where_statement}
        ;
        """
    df = pd.read_sql(sel_query, database_connection.engine)
    return df


def query_collections(database_connection: PostgresCon) -> pd.DataFrame:
    sel_query = """SELECT
        *
        FROM
        store_collections
        ;
        """
    df = pd.read_sql(sel_query, database_connection.engine)
    return df


def query_categories(database_connection: PostgresCon) -> pd.DataFrame:
    sel_query = """SELECT
        *
        FROM
        store_categories
        ;
        """
    df = pd.read_sql(sel_query, database_connection.engine)
    return df


def query_countries(database_connection: PostgresCon) -> pd.DataFrame:
    sel_query = """SELECT
        *
        FROM
        countries
        ;
        """
    df = pd.read_sql(sel_query, database_connection.engine)
    return df


def query_store_ids(
    database_connection: PostgresCon,
    store: int,
    store_ids: list[str] | None = None,
) -> list[str]:
    df = query_store_id_map(
        database_connection=database_connection,
        store=store,
        store_ids=store_ids,
    )
    ids: list = []
    if not df.empty:
        ids = df["store_id"].tolist()
    else:
        ids = []
    return ids


def query_pub_domains(
    database_connection: PostgresCon,
    limit: None | int = 10000,
    exclude_recent_days: int = 2,
) -> pd.DataFrame:
    """Query pub domains
    that have apps which are ad supported and still on store
    params: limit: int number of rows to return
    """
    limit_str = ""
    exclude_str = ""
    if exclude_recent_days:
        before_date = (
            datetime.datetime.today() - datetime.timedelta(days=exclude_recent_days)
        ).strftime("%Y-%m-%d")
        exclude_str = f"AND (pd.crawled_at <= '{before_date}' OR pd.crawled_at IS NULL)"
    if limit:
        limit_str = f"LIMIT {limit}"
    sel_query = f"""SELECT
            DISTINCT pd.id, pd.url, pd.crawled_at
        FROM
            app_urls_map aum
        LEFT JOIN pub_domains pd ON
            pd.id = aum.pub_domain
        LEFT JOIN store_apps sa ON
            sa.id = aum.store_app
        WHERE
            sa.ad_supported
            AND sa.crawl_result = 1
            {exclude_str}
        ORDER BY
            pd.crawled_at NULLS FIRST
        {limit_str}
        ; 
        """
    df = pd.read_sql(sel_query, database_connection.engine)
    return df


def delete_app_url_mapping(app_url_id: int, database_connection: PostgresCon) -> None:
    del_query = text("DELETE FROM app_urls_map WHERE id = :app_url_id")
    logger.info(f"{app_url_id=} delete app_urls_map start")

    with database_connection.engine.connect() as conn:
        conn.execute(del_query, {"app_url_id": app_url_id})
        conn.commit()


def query_store_apps(
    stores: list[int],
    database_connection: PostgresCon,
    group: str = "short",
    limit: int | None = 1000,
) -> pd.DataFrame:
    short_update_days = 6
    short_update_installs = 1000
    short_update_ratings = 100
    short_update_date = (
        datetime.datetime.now(tz=datetime.UTC)
        - datetime.timedelta(days=short_update_days)
    ).strftime("%Y-%m-%d")
    long_update_days = 13
    long_update_date = (
        datetime.datetime.now(tz=datetime.UTC)
        - datetime.timedelta(days=long_update_days)
    ).strftime("%Y-%m-%d")
    max_recrawl_days = 29
    max_recrawl_date = (
        datetime.datetime.now(tz=datetime.UTC)
        - datetime.timedelta(days=max_recrawl_days)
    ).strftime("%Y-%m-%d")
    long_group = f"""(
                            sa.updated_at <= '{long_update_date}'
                            AND (
                                installs < {short_update_installs} 
                                OR rating_count < {short_update_ratings}
                            )
                            AND (crawl_result = 1 OR crawl_result IS NULL)
                           )
                    """
    short_group = f"""(
                            (
                             installs >= {short_update_installs}
                             OR rating_count >= {short_update_ratings}
                            )
                            AND sa.updated_at <= '{short_update_date}'
                            AND (crawl_result = 1 OR crawl_result IS NULL)
                        )
                        """
    max_group = f"""(
                        sa.updated_at <= '{max_recrawl_date}'
                        OR crawl_result IS NULL
                        )
        """
    if group == "short":
        installs_and_dates_str = short_group
    elif group == "long":
        installs_and_dates_str = long_group
    elif group == "max":
        installs_and_dates_str = max_group
    else:
        installs_and_dates_str = f"""(
                        {short_group}
                        OR {long_group}
                        OR {max_group}
                        )
                        """
    where_str = "store IN (" + (", ").join([str(x) for x in stores]) + ")"
    where_str += f""" AND {installs_and_dates_str}"""
    limit_str = ""
    if limit:
        limit_str = f"LIMIT {limit}"
    sel_query = f"""SELECT 
            store, 
            sa.id as store_app, 
            store_id, 
            sa.updated_at, 
            aum.id as app_url_id  
        FROM 
            store_apps sa
        LEFT JOIN app_urls_map aum
            ON aum.store_app = sa.id
        WHERE {where_str}
        ORDER BY
            (CASE
                WHEN crawl_result IS NULL THEN 0
                ELSE 1
            END),
            --sa.updated_at
            COALESCE(review_count, 0
                ) + 
            GREATEST(
                    COALESCE(installs, 0),
                    COALESCE(CAST(rating_count AS bigint), 0)*50
                )
            DESC NULLS LAST
        {limit_str}
        """
    df = pd.read_sql(sel_query, database_connection.engine)
    return df


def query_all(
    table_name: str,
    key_cols: list[str] | str,
    df: pd.DataFrame,
    database_connection: PostgresCon,
) -> pd.DataFrame:
    if isinstance(key_cols, str):
        key_cols = [key_cols]
    wheres = []
    for key_col in key_cols:
        keys = df[key_col].unique().tolist()
        if all([isinstance(x, np.integer | int) for x in keys]):
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


def get_top_ranks_for_unpacking(
    database_connection: PostgresCon,
    store: int,
    collection_id: int,
    limit: int = 25,
) -> pd.DataFrame:
    sel_query = f"""WITH topranks AS(
                SELECT
                     ar.RANK,
                     ar.store_category,
                     sa.id AS store_app,
                     sa.name,
                     sa.installs,
                     sa.rating_count,
                     sa.store_id
                FROM
                     app_rankings ar
                LEFT JOIN
                     stores s ON
                     s.id = ar.store
                LEFT JOIN
                     store_apps sa ON
                     sa.id = ar.store_app
                WHERE
                     crawled_date = (
                        SELECT
                            max(crawled_date)
                        FROM
                            app_rankings
                        WHERE
                            store = {store}
                    )
                    AND ar.store = {store}
                    AND ar.store_collection = {collection_id}
                ORDER BY
                    ar.rank,
                        store_category
            ),
                    distinctapps AS (
                SELECT
                    DISTINCT topranks.store_app,
                    topranks.name,
                    topranks.store_id,
                    topranks.installs,
                    topranks.rating_count
                FROM
                    topranks
            ),
            latest_version_codes AS (
                SELECT
                    DISTINCT ON
                    (version_codes.store_app)
                    version_codes.id,
                    version_codes.store_app,
                    version_codes.version_code,
                    version_codes.updated_at,
                    version_codes.crawl_result
                FROM
                    version_codes
                ORDER BY
                    version_codes.store_app,
                    string_to_array(version_codes.version_code, '.')::bigint[] DESC
            ),
            scheduled_apps_crawl AS (
            SELECT
                dc.store_app,
                dc.store_id,
                dc.name,
                dc.installs,
                dc.rating_count,
                vc.crawl_result as last_crawl_result,
                vc.updated_at
            FROM
                distinctapps dc
            LEFT JOIN latest_version_codes vc ON
                vc.store_app = dc.store_app
            WHERE
                vc.updated_at IS NULL OR
                (
                (vc.crawl_result = 1 AND vc.updated_at < current_date - INTERVAL '60 days')
                OR
                (vc.crawl_result IN (2,3,4) AND vc.updated_at < current_date - INTERVAL '30 days')
                )
            ORDER BY
            (CASE
                WHEN crawl_result IS NULL THEN 0
                ELSE 1
            END),
            GREATEST(
                    COALESCE(installs, 0),
                    COALESCE(CAST(rating_count AS bigint), 0)*50
                )
            DESC NULLS LAST
            ),
            user_requested_apps_crawl AS (
            SELECT
                DISTINCT sa.id AS store_app,
                sa.store_id,
                sa.name,
                sa.installs,
                sa.rating_count,
                lvs.crawl_result AS last_crawl_result,
                lvs.updated_at
            FROM
                user_requested_scan urs
            LEFT JOIN store_apps sa ON
                urs.store_id = sa.store_id
            LEFT JOIN latest_version_codes lvs ON
                sa.id = lvs.id
            WHERE
                lvs.updated_at > urs.created_at
                    OR lvs.updated_at IS NULL
            )
        SELECT
            *
        FROM
            user_requested_apps_crawl
        UNION ALL
        SELECT
            *
        FROM
            scheduled_apps_crawl
        LIMIT {limit}
        ;
    """
    df = pd.read_sql(sel_query, con=database_connection.engine)
    return df
