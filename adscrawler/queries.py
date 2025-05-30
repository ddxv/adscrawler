import datetime
from functools import lru_cache

import numpy as np
import pandas as pd
from psycopg import Connection
from psycopg.sql import SQL, Composed, Identifier
from sqlalchemy import text

from .config import get_logger
from .connection import PostgresCon

logger = get_logger(__name__)


def insert_df(
    df: pd.DataFrame,
    table_name: str,
    database_connection: Connection,
    insert_columns: list[str] | None = None,
    return_rows: bool = False,
    schema: str | None = None,
    log: bool = False,
) -> pd.DataFrame | None:
    """Perform an "insert" on a PostgreSQL table from a DataFrame.
    Constructs an INSERT statement, uploads the DataFrame to a
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
    """

    raw_conn = database_connection.engine.raw_connection()

    if "crawled_date" in df.columns and df["crawled_date"].isna().all():
        df["crawled_date"] = pd.to_datetime(df["crawled_date"]).dt.date
        df["crawled_date"] = None
    if "release_date" in df.columns and df["release_date"].isna().all():
        df["release_date"] = None

    if insert_columns is None:
        insert_columns = df.columns.tolist()
    all_columns = list(set(insert_columns))
    table_identifier = Identifier(table_name)
    if schema:
        table_identifier = Composed([Identifier(schema), SQL("."), table_identifier])

    columns = SQL(", ").join(map(Identifier, all_columns))
    placeholders = SQL(", ").join(SQL("%s") for _ in all_columns)

    if return_rows:
        returning_clause = SQL("RETURNING * ")
    else:
        returning_clause = SQL("")

    insert_query = SQL("""
        INSERT INTO {table} ({columns})
        VALUES ({placeholders})
        {returning_clause}
    """).format(
        table=table_identifier,
        columns=columns,
        placeholders=placeholders,
        returning_clause=returning_clause,
    )

    if log:
        logger.info(f"Insert query: {insert_query.as_string(raw_conn)}")

    results = []
    column_names = None
    with raw_conn.cursor() as cur:
        data = [
            tuple(row) for row in df[all_columns].itertuples(index=False, name=None)
        ]
        if log:
            logger.info(f"Insert data: {data}")
        for row in data:
            cur.execute(insert_query, row)
            if return_rows:
                results.append(cur.fetchone())
                if column_names is None:
                    column_names = [desc[0] for desc in cur.description]
        raw_conn.commit()

    if return_rows:
        return_df = pd.DataFrame(results, columns=column_names)
    else:
        return_df = None
    return return_df


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
        if log:
            logger.info(f"Upsert data: {data}")
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


def insert_version_code(
    version_str: str,
    store_app: int,
    crawl_result: int,
    database_connection: PostgresCon,
    apk_hash: str | None = None,
    return_rows: bool = False,
) -> pd.DataFrame | None:
    version_code_df = pd.DataFrame(
        [
            {
                "store_app": store_app,
                "version_code": version_str,
                "apk_hash": apk_hash,
                "crawl_result": crawl_result,
            }
        ]
    )
    try:
        log_download_crawl_results(version_code_df, database_connection)
    except Exception as e:
        logger.error(f"{store_app=} {version_str=} {crawl_result=} {apk_hash=} {e=}")
        raise e

    upserted: pd.DataFrame = upsert_df(
        df=version_code_df,
        table_name="version_codes",
        database_connection=database_connection,
        key_columns=["store_app", "version_code"],
        return_rows=return_rows,
        insert_columns=["store_app", "version_code", "crawl_result", "apk_hash"],
    )
    logger.info(f"{store_app=} {version_str=} inserted to db")

    if return_rows:
        upserted = upserted.rename(
            columns={"version_code": "original_version_code", "id": "version_code"}
        ).drop("store_app", axis=1)
    return upserted


def log_download_crawl_results(
    df: pd.DataFrame, database_connection: PostgresCon
) -> None:
    insert_columns = ["store_app", "version_code", "crawl_result"]
    df = df[insert_columns]
    df.to_sql(
        name="store_app_downloads",
        schema="logging",
        con=database_connection.engine,
        if_exists="append",
        index=False,
    )


def log_version_code_scan_crawl_results(
    store_app: int,
    md5_hash: str,
    crawl_result: int,
    database_connection: PostgresCon,
    version_code_id: int | None = None,
) -> None:
    df = pd.DataFrame(
        {
            "store_app": [store_app],
            "version_code": [version_code_id],
            "apk_hash": [md5_hash],
            "crawl_result": [crawl_result],
        }
    )
    df.to_sql(
        name="version_code_api_scan_results",
        schema="logging",
        con=database_connection.engine,
        if_exists="append",
        index=False,
    )
    logger.info(f"logged: {store_app=} {md5_hash=} {crawl_result=} {version_code_id=}")


def upsert_details_df(
    details_df: pd.DataFrame,
    database_connection: PostgresCon,
    store_id: str,
    raw_txt_str: str,
) -> None:
    details_df = details_df.rename(
        columns={
            "path": "xml_path",
            "android_name": "value_name",
            "version_code_id": "version_code",
        }
    )
    key_insert_columns = ["xml_path", "tag", "value_name"]
    logger.info(f"{store_id=} insert {details_df.shape[0]} version_strings to db")
    details_df.loc[details_df["tag"].isna(), "tag"] = ""
    strings_df = details_df[key_insert_columns + ["version_code"]].drop_duplicates()
    version_strings_df = upsert_df(
        df=strings_df,
        table_name="version_strings",
        database_connection=database_connection,
        key_columns=key_insert_columns,
        insert_columns=key_insert_columns,
        return_rows=True,
    )
    if version_strings_df is None:
        logger.error(f"{store_id=} insert version_strings to db returned None")
        logger.error(strings_df[strings_df["tag"].isna()])
        raise Exception(f"{store_id=} insert version_strings to db")
    version_strings_df = version_strings_df.rename(columns={"id": "string_id"})
    strings_map_df = pd.merge(
        strings_df,
        version_strings_df,
        how="left",
        on=["xml_path", "tag", "value_name"],
        validate="many_to_one",
    )
    if strings_map_df["string_id"].isna().any():
        logger.error(f"{store_id=} insert strings_map to db")
        logger.error(strings_map_df[strings_map_df["string_id"].isna()])
    insert_columns = ["version_code", "string_id"]
    upsert_df(
        table_name="version_details_map",
        insert_columns=insert_columns,
        df=strings_map_df,
        key_columns=insert_columns,
        database_connection=database_connection,
    )
    strings_map_df["manifest_string"] = raw_txt_str
    manifest_df = strings_map_df[["version_code", "manifest_string"]].drop_duplicates()
    upsert_df(
        df=manifest_df,
        table_name="version_manifests",
        database_connection=database_connection,
        key_columns=["version_code"],
        insert_columns=["version_code", "manifest_string"],
    )
    logger.info(f"{store_id=} finished")


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


@lru_cache(maxsize=1)
def query_countries(database_connection: PostgresCon) -> pd.DataFrame:
    sel_query = """SELECT
        *
        FROM
        countries
        ;
        """
    df = pd.read_sql(sel_query, database_connection.engine)
    return df


@lru_cache(maxsize=1)
def query_languages(database_connection: PostgresCon) -> pd.DataFrame:
    sel_query = """SELECT
        *
        FROM
        languages
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


def query_keywords_to_crawl(
    database_connection: PostgresCon,
    limit: int | None = None,
) -> pd.DataFrame:
    limit_str = ""
    if limit:
        limit_str = f"LIMIT {limit}"
    sel_query = f"""WITH rank_crawled_keywords AS (
                SELECT
                    DISTINCT akr.keyword
                FROM
                    app_keyword_rankings akr
                WHERE
                    akr.crawled_date > CURRENT_DATE - INTERVAL '7 days'
            ),
            log_crawled_keywords AS (
                SELECT
                    DISTINCT keyword
                FROM
                    logging.keywords_crawled_at
                WHERE
                    crawled_at > CURRENT_DATE - INTERVAL '7 days'
            )
            SELECT
                *
            FROM
                frontend.keyword_scores ks
            WHERE
                ks.keyword_id NOT IN (
                    SELECT
                        keyword
                    FROM
                        rank_crawled_keywords
                )
                OR ks.keyword_id NOT IN (
                    SELECT
                        keyword
                    FROM
                        log_crawled_keywords
                )
            ORDER BY
                ks.competitiveness_score
            DESC
            {limit_str}
            ;
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


def query_apps_to_download(
    database_connection: PostgresCon,
    store: int,
    limit: int = 25,
) -> pd.DataFrame:
    sel_query = f"""
    WITH latest_version_codes AS (
            SELECT
                    DISTINCT ON
                    (store_app)
                    id,
                    store_app,
                    version_code,
                    updated_at,
                    crawl_result
                FROM
                    version_codes
                ORDER BY
                    store_app,
                    updated_at DESC,
                    string_to_array(version_code, '.')::bigint[] DESC
            ),
            latest_success_version_codes AS (
                SELECT
                    DISTINCT ON
                    (store_app)
                    id,
                    store_app,
                    version_code,
                    updated_at,
                    crawl_result
                FROM
                    version_codes
                WHERE crawl_result = 1
                ORDER BY
                    store_app,
                    updated_at DESC,
                    string_to_array(version_code, '.')::bigint[] DESC
            ),
            failing_downloads AS (
                SELECT
                    store_app,
                    count(*) AS attempt_count
                FROM
                    logging.store_app_downloads
                WHERE
                    crawl_result != 1
                    AND updated_at >= current_date - INTERVAL '30 days'
                GROUP BY
                    store_app
            ),       
            scheduled_apps_crawl AS (
                SELECT
                    dc.store_app,
                    dc.store_id,
                    dc.name,
                    dc.installs,
                    dc.rating_count,
                    COALESCE(fd.attempt_count, 0) AS attempt_count,
                    vc.crawl_result as last_crawl_result,
                    vc.updated_at AS last_download_attempt,
                    lsvc.updated_at AS last_downloaded_at
                FROM
                    store_apps_in_latest_rankings dc
                LEFT JOIN latest_version_codes vc ON
                    vc.store_app = dc.store_app
                LEFT JOIN latest_success_version_codes lsvc ON 
                   dc.store_app = lsvc.store_app
                LEFT JOIN failing_downloads fd ON 
                    vc.store_app = fd.store_app
                WHERE
                    dc.store = {store}
                    AND
                    (   
                        vc.updated_at IS NULL 
                        OR
                            (
                            (vc.crawl_result = 1 AND vc.updated_at < current_date - INTERVAL '180 days')
                            OR
                            (lsvc.updated_at IS NULL AND vc.crawl_result IN (2,3,4) AND vc.updated_at < current_date - INTERVAL '30 days')
                            )
                    )
            ),
            user_requested_apps_crawl AS (
            SELECT
                DISTINCT ON (sa.id)
                sa.id AS store_app,
                sa.store_id,
                sa.name,
                sa.installs,
                sa.rating_count,
                COALESCE(fd.attempt_count, 0) AS attempt_count,
                urs.created_at AS user_last_requested,
                lvc.crawl_result AS last_crawl_result,
                lvc.updated_at AS last_download_attempt,
                lsvc.updated_at AS last_downloaded_at
            FROM
                user_requested_scan urs
            LEFT JOIN store_apps sa ON
                urs.store_id = sa.store_id
            LEFT JOIN latest_success_version_codes lsvc ON
                sa.id = lsvc.store_app
            LEFT JOIN latest_version_codes lvc ON
                sa.id = lvc.store_app
            LEFT JOIN failing_downloads fd ON sa.id = fd.store_app
            WHERE
                (
                    (lsvc.updated_at < urs.created_at 
                       AND (
                       (sa.store_last_updated > lsvc.updated_at AND lsvc.updated_at > '2025-05-01')
                       OR lsvc.updated_at < '2025-05-01'
                       )
                    )
                    OR lsvc.updated_at IS NULL
                )
                AND (lvc.updated_at < current_date - INTERVAL '1 days'
                    OR lvc.updated_at IS NULL)
                AND sa.store = {store}
                AND sa."free"
                AND sa.name IS NOT NULL -- random apps requested directly BY users NOT FOUND ON store
            ORDER BY sa.id, urs.created_at desc       
),
        combined AS (SELECT
            store_app,
                store_id,
                name,
                installs,
                rating_count,
                attempt_count,
                last_crawl_result,
                'user' AS mysource,
                last_download_attempt,
                last_downloaded_at
        FROM
            user_requested_apps_crawl
        WHERE attempt_count < 3
        UNION ALL
        SELECT
                    store_app,
                    store_id,
                    name,
                    installs,
                    rating_count,
                    attempt_count,
                    last_crawl_result,
                    'scheduled' AS mysource,
                    last_download_attempt,
                    last_downloaded_at
        FROM
            scheduled_apps_crawl
        WHERE attempt_count < 2
        )
        SELECT *  FROM combined
        ORDER BY
                mysource DESC,
                (CASE
                    WHEN last_crawl_result IS NULL THEN 0
                    ELSE 1
                END),
                GREATEST(
                        COALESCE(installs, 0),
                        COALESCE(CAST(rating_count AS bigint), 0)*50
                    )
                DESC NULLS LAST
        LIMIT {limit}
        ;
    """
    df = pd.read_sql(sel_query, con=database_connection.engine)
    return df


def query_apps_to_sdk_scan(
    database_connection: PostgresCon,
    store: int,
    limit: int = 25,
) -> pd.DataFrame:
    sel_query = f"""
    WITH latest_version_codes AS (
                SELECT
                    DISTINCT ON
                    (store_app)
                    id,
                    store_app,
                    version_code,
                    updated_at AS last_downloaded_at,
                    crawl_result AS download_result
                FROM
                    version_codes
                WHERE crawl_result = 1
                -- HACKY FIX only try for apps that have successuflly been downloaded, but this table still is all history of version_codes in general
                AND updated_at >= '2025-05-01'
                ORDER BY
                    store_app,
                    updated_at DESC,
                    string_to_array(version_code, '.')::bigint[] DESC
            ),
            last_scan AS (
                SELECT
                    DISTINCT ON
                    (vc.store_app)
                    vc.store_app,
                    version_code_id AS version_code,
                    scanned_at,
                    scan_result
                FROM
                    version_code_sdk_scan_results lsscr
                LEFT JOIN version_codes vc ON lsscr.version_code_id = vc.id
                ORDER BY
                    vc.store_app,
                    lsscr.scanned_at DESC 
            ),
            last_scan_succeed AS (
                SELECT
                    DISTINCT ON
                    (vc.store_app)
                    vc.id,
                    vc.store_app,
                    vc.version_code,
                    vcss.scanned_at,
                    vcss.scan_result
                FROM
                    version_codes vc
                LEFT JOIN version_code_sdk_scan_results vcss ON 
                vc.id = vcss.version_code_id
                WHERE vcss.scan_result = 1 
                ORDER BY
                    vc.store_app,
                    vcss.scanned_at DESC,
                    string_to_array(vc.version_code, '.')::bigint[] DESC
            ),
            scheduled_apps_crawl AS (
            SELECT
                dc.store_app,
                dc.store_id,
                dc.name,
                dc.installs,
                dc.rating_count,
                'regular' AS mysource,
                ls.scan_result AS last_analyzed_result,
                ls.scanned_at AS last_scanned_at,
                lsvc.scanned_at AS last_scuccess_scanned_at
            FROM
                store_apps_in_latest_rankings dc
            RIGHT JOIN latest_version_codes vc ON
                vc.store_app = dc.store_app
            LEFT JOIN last_scan ls ON dc.store_app = ls.store_app
            LEFT JOIN last_scan_succeed lsvc ON
                dc.store_app = lsvc.store_app
            WHERE
                dc.store = {store}
                AND vc.download_result = 1
                AND
                (   
                    ls.scanned_at IS NULL 
                    OR
                        (
                        (lsvc.scan_result = 1 AND lsvc.scanned_at < current_date - INTERVAL '30 days')
                        OR
                        (lsvc.scan_result IN (2,3,4) AND (lsvc.scanned_at < current_date - INTERVAL '10 days' AND ls.scanned_at < current_date - INTERVAL '3 days'))
                        )
                )
            ORDER BY
            (CASE
                WHEN download_result IS NULL THEN 0
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
                DISTINCT ON (sa.id)
                sa.id AS store_app,
                sa.store_id,
                sa.name,
                sa.installs,
                sa.rating_count,
                'user' AS mysource,
                ls.scan_result AS last_analyzed_result,
                ls.scanned_at AS last_scanned_at,
                lsvc.scanned_at AS last_scuccess_scanned_at
            FROM
                user_requested_scan urs
            LEFT JOIN store_apps sa ON
                urs.store_id = sa.store_id
            LEFT JOIN last_scan ls ON sa.id = ls.store_app
            LEFT JOIN last_scan_succeed lsvc ON
                sa.id = lsvc.store_app                
            RIGHT JOIN latest_version_codes lvc ON
                sa.id = lvc.store_app
            WHERE
                (lsvc.scanned_at < urs.created_at
                    OR lsvc.scanned_at IS NULL
                )
                AND (lvc.last_downloaded_at < current_date - INTERVAL '1 days'
                    OR lvc.last_downloaded_at IS NULL)
                AND sa.store = {store}
            ORDER BY sa.id, urs.created_at desc
            )
        SELECT
            store_app,
            store_id,
            name,
            installs,
            rating_count,
            mysource,
            last_analyzed_result,
            last_scanned_at,
            last_scuccess_scanned_at
        FROM
            user_requested_apps_crawl
        UNION ALL
        SELECT
            store_app,
            store_id,
            name,
            installs,
            rating_count,
            mysource,
           last_analyzed_result,
            last_scanned_at,
            last_scuccess_scanned_at
        FROM
            scheduled_apps_crawl
            LIMIT {limit}
        ;
    """
    df = pd.read_sql(sel_query, con=database_connection.engine)
    return df


def query_apps_to_api_check(
    database_connection: PostgresCon,
    store: int,
) -> pd.DataFrame:
    sel_query = f"""WITH latest_version_codes AS (
                SELECT
                    DISTINCT ON
                    (store_app)
                    id,
                    store_app,
                    version_code,
                    updated_at AS last_downloaded_at,
                    crawl_result AS download_result
                FROM
                    version_codes
                WHERE crawl_result = 1
                -- HACKY FIX only try for apps that have successuflly been downloaded, but this table still is all history of version_codes in general
                AND updated_at >= '2025-05-01'
                ORDER BY
                    store_app,
                    updated_at DESC,
                    string_to_array(version_code, '.')::bigint[] DESC
            ),
            last_scanned AS (
                    SELECT
                        DISTINCT ON
                        (vc.store_app) *
                    FROM
                       version_code_api_scan_results vasr
                    LEFT JOIN version_codes vc 
                        ON vasr.version_code_id = vc.id
                    WHERE
                        vc.updated_at >= '2025-05-01'
                    ORDER BY vc.store_app, vasr.run_at desc
                ),
            last_successful_scanned AS (
                    SELECT
                        DISTINCT ON
                        (vc.store_app) *
                    FROM
                       version_code_api_scan_results vasr
                    LEFT JOIN version_codes vc 
                        ON vasr.version_code_id = vc.id
                    WHERE
                        vc.crawl_result = 1
                        AND vc.updated_at >= '2025-05-01'
                    ORDER BY vc.store_app, vasr.run_at desc
                ),    
            failed_runs AS (
            SELECT store_app, count(*) AS failed_attempts FROM logging.version_code_api_scan_results
            WHERE crawl_result != 1
                AND updated_at >= current_date - INTERVAL '10 days'
                GROUP BY store_app
),
        scheduled_to_run AS (
                SELECT lvc.store_app as store_app,
                sa.name,
                sa.store_id,
                sa.installs,
                ls.run_at AS last_run_at,
                fr.failed_attempts,
                ls.run_result AS last_run_result,
                lss.run_at AS last_succesful_run_at,
                lvc.last_downloaded_at
                FROM
                    latest_version_codes lvc
                LEFT JOIN last_scanned ls ON
                    lvc.store_app = ls.store_app
                LEFT JOIN last_successful_scanned lss ON lvc.id = lss.version_code_id
                LEFT JOIN store_apps sa ON
                    lvc.store_app = sa.id
                LEFT JOIN failed_runs fr ON sa.id = fr.store_app
                WHERE (ls.run_at <= CURRENT_DATE - INTERVAL '120 days' OR ls.run_at IS NULL)
                    AND sa.store = {store}
                      AND (fr.failed_attempts < 1 OR fr.failed_attempts IS NULL )
                ORDER BY sa.installs DESC
                ),     
        user_requested_apps_crawl AS (
            SELECT
                DISTINCT ON (sa.id) 
                sa.id AS store_app,
                sa.store_id,
                sa.name,
                sa.installs,
                ls.run_at AS last_run_at,
                fr.failed_attempts,
                ls.run_result AS last_run_result,
                lss.run_at AS last_succesful_run_at,
                lsvc.last_downloaded_at,
                urs.created_at AS user_requested_at
            FROM
                user_requested_scan urs
            LEFT JOIN store_apps sa ON
                urs.store_id = sa.store_id
            INNER JOIN latest_version_codes lsvc ON
                sa.id = lsvc.store_app
            LEFT JOIN last_scanned ls ON lsvc.id = ls.version_code_id
            LEFT JOIN last_successful_scanned lss ON lsvc.id = lss.version_code_id
            LEFT JOIN failed_runs fr ON sa.id = fr.store_app
            WHERE
                (ls.run_at < urs.created_at
                    OR ls.run_at IS NULL
                )
                AND sa.store = {store}
                AND (fr.failed_attempts < 1 OR fr.failed_attempts IS NULL 
                )
            ORDER BY sa.id, urs.created_at desc
            )
            SELECT 
                store_app,
                store_id,
                name,
                installs,
                last_run_at,
                failed_attempts,
                last_run_result,
                last_succesful_run_at,
                last_downloaded_at,
                user_requested_at,
                'user' AS mysource
                FROM user_requested_apps_crawl urac
            UNION ALL
            SELECT 
                store_app,
                store_id,
                name,
                installs,
                last_run_at,
                last_run_result,
                failed_attempts,
                last_succesful_run_at,
                last_downloaded_at,
                NULL AS user_requested_at,
                'scheduled' AS mysource
                FROM scheduled_to_run
                WHERE store_id NOT IN (SELECT store_id FROM user_requested_apps_crawl)
            ORDER BY mysource DESC, user_requested_at, installs DESC 
            ; 
        """
    df = pd.read_sql(sel_query, database_connection.engine)
    return df


def query_all_store_app_descriptions(
    database_connection: PostgresCon,
) -> pd.DataFrame:
    sel_query = """SELECT
    DISTINCT ON (store_app)
        description
    FROM
        store_apps_descriptions
        ORDER BY store_app, updated_at desc
    ;
    """
    df = pd.read_sql(sel_query, con=database_connection.engine)
    return df


@lru_cache(maxsize=1)
def query_keywords_base(database_connection: PostgresCon) -> pd.DataFrame:
    sel_query = """SELECT
    k.keyword_text
    FROM
    keywords_base kb
    LEFT JOIN keywords k ON
        k.id = kb.keyword_id
    ;
    """
    df = pd.read_sql(sel_query, con=database_connection.engine)
    df["keyword_text"] = " " + df["keyword_text"] + " "
    return df


def query_store_app_by_store_id(
    database_connection: PostgresCon,
    store_id: str,
) -> int:
    sel_query = f"""SELECT * FROM store_apps WHERE store_id = '{store_id}'"""
    df = pd.read_sql(sel_query, con=database_connection.engine)
    if df.empty:
        raise ValueError(f"Store id {store_id} not found")
    try:
        return int(df.iloc[0]["id"])
    except Exception:
        logger.exception(f"Error getting store app id for {store_id}")
        raise


def get_version_codes_full_history(
    database_connection: PostgresCon, store_ids: list[str]
) -> pd.DataFrame:
    sel_query = """SELECT vc.*, sa.store_id, sa.name as app_name FROM version_codes vc
    LEFT JOIN store_apps sa ON
        sa.id = vc.store_app
    WHERE sa.store_id IN ({})""".format(
        ", ".join([f"'{store_id}'" for store_id in store_ids])
    )
    df = pd.read_sql(sel_query, con=database_connection.engine)
    return df


def get_version_code_dbid(
    store_app: int, version_code: str, database_connection: PostgresCon
) -> int | None:
    sel_query = f"""SELECT * FROM version_codes WHERE store_app = {store_app} AND version_code = '{version_code}'"""
    df = pd.read_sql(sel_query, con=database_connection.engine)
    if df.empty:
        insert_version_code(
            version_str=version_code,
            store_app=store_app,
            crawl_result=4,
            database_connection=database_connection,
        )
        return None
    try:
        return int(df.iloc[0]["id"])
    except Exception:
        logger.exception(
            f"Error getting version code id for {store_app} and {version_code}"
        )
        raise


def get_version_code_by_md5_hash(
    database_connection: PostgresCon, md5_hash: str, store_id: str
) -> int | None:
    sel_query = f"""SELECT * FROM version_codes vc
    LEFT JOIN store_apps sa ON
        sa.id = vc.store_app
    WHERE apk_hash = '{md5_hash}' AND sa.store_id = '{store_id}'
    AND vc.crawl_result = 1
    ;
    """
    df = pd.read_sql(sel_query, con=database_connection.engine)
    if df.empty:
        return None
    try:
        return int(df.iloc[0]["id"])
    except Exception:
        logger.exception(f"Error getting version code id for {md5_hash} and {store_id}")
        raise


def get_all_api_calls(database_connection: PostgresCon) -> pd.DataFrame:
    sel_query = """SELECT * FROM store_app_api_calls;"""
    df = pd.read_sql(sel_query, con=database_connection.engine)
    return df
