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


def query_store_id_api_called_map(
    database_connection: PostgresCon,
    store: int | None = None,
    store_ids: list[str] | None = None,
    exclude_recently_crawled: bool = True,
) -> pd.DataFrame:
    where_statement = ""
    if store or store_ids or exclude_recently_crawled:
        if store:
            where_statement += f"WHERE store = {store} "
        if store_ids:
            if "WHERE" not in where_statement:
                where_statement += " WHERE "
            else:
                where_statement += " AND "
            store_ids_list = "'" + "','".join(store_ids) + "'"
            where_statement += f" sa.store_id in ({store_ids_list}) "
        if exclude_recently_crawled:
            if "WHERE" not in where_statement:
                where_statement += " WHERE "
            else:
                where_statement += " AND "
            where_statement += "(ml.crawled_at <= CURRENT_DATE - INTERVAL '30 days' OR ml.crawled_at IS NULL)"
    sel_query = f"""
                WITH max_logging AS (
                    SELECT
                        DISTINCT ON
                        (store_app) *
                    FROM
                        logging.store_app_waydroid_crawled_at
                    WHERE
                        crawled_at IS NOT NULL
                    ORDER BY
                        store_app,
                        crawled_at DESC
                ),
                max_api_calls AS (
                    SELECT
                        DISTINCT ON
                        (store_app) *
                    FROM
                        store_app_api_calls
                    WHERE
                        crawled_at IS NOT NULL
                    ORDER BY
                        store_app,
                        crawled_at DESC
                )
                SELECT sa.id as store_app, 
                sa.store_id, 
                ml.crawled_at
                FROM
                    store_apps sa
                LEFT JOIN max_logging ml ON
                    sa.id = ml.store_app
                LEFT JOIN max_api_calls mac ON
                    sa.id = mac.store_app
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


def get_top_apps_to_download(
    database_connection: PostgresCon,
    store: int,
    limit: int = 25,
) -> pd.DataFrame:
    sel_query = f"""WITH
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
                    version_codes.updated_at DESC,
                    string_to_array(version_codes.version_code, '.')::bigint[] DESC
            ),
             latest_success_version_codes AS (
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
                WHERE version_codes.crawl_result = 1
                ORDER BY
                    version_codes.store_app,
                    version_codes.updated_at DESC,
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
                store_apps_in_latest_rankings dc
            LEFT JOIN latest_version_codes vc ON
                vc.store_app = dc.store_app
            WHERE
                dc.store = {store}
                AND
                (   
                    vc.updated_at IS NULL 
                    OR
                        (
                        (vc.crawl_result = 1 AND vc.updated_at < current_date - INTERVAL '180 days')
                        OR
                        (vc.crawl_result IN (2,3,4) AND vc.updated_at < current_date - INTERVAL '30 days')
                        )
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
                lsvc.crawl_result AS last_crawl_result,
                lsvc.updated_at
            FROM
                user_requested_scan urs
            LEFT JOIN store_apps sa ON
                urs.store_id = sa.store_id
            LEFT JOIN latest_success_version_codes lsvc ON
                sa.id = lsvc.store_app
            LEFT JOIN latest_version_codes lvc ON
                sa.id = lvc.store_app
            WHERE
                (lsvc.updated_at < urs.created_at
                    OR lsvc.updated_at IS NULL
                )
                AND (lvc.updated_at < current_date - INTERVAL '1 days'
                    OR lvc.updated_at IS NULL)
                AND sa.store = {store}
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


def get_next_to_sdk_scan(
    database_connection: PostgresCon,
    store: int,
    limit: int = 25,
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
            latest_analyze_results AS (
                SELECT
                    DISTINCT ON
                    (version_code)
                    version_code,
                    sdk_analyzed_at AS sd_last_analyzed,
                    analyze_result AS analyze_result
                FROM
                    version_details_map
                WHERE analyze_result = 1
                ORDER BY
                    version_code DESC
            ),
             latest_success_version_codes AS (
                SELECT
                    DISTINCT ON
                    (vc.store_app)
                    vc.id,
                    vc.store_app,
                    vc.version_code,
                    vdm.sdk_analyzed_at,
                    vdm.analyze_result
                FROM
                    version_codes vc
                LEFT JOIN version_details_map vdm ON 
                vc.id = vdm.version_code
                WHERE vdm.analyze_result = 1 
                ORDER BY
                    vc.store_app,
                    vdm.sdk_analyzed_at DESC,
                    string_to_array(vc.version_code, '.')::bigint[] DESC
            ),
            scheduled_apps_crawl AS (
            SELECT
                dc.store_app,
                dc.store_id,
                dc.name,
                dc.installs,
                dc.rating_count,
                vc.download_result as download_result,
               lsvc.analyze_result AS last_analyzed_result,
                lsvc.sdk_analyzed_at
            FROM
                store_apps_in_latest_rankings dc
            RIGHT JOIN latest_version_codes vc ON
                vc.store_app = dc.store_app
            LEFT JOIN latest_success_version_codes lsvc ON
                dc.store_app = lsvc.store_app
            WHERE
                dc.store = {store}
                AND vc.download_result = 1
                AND
                (   
                    lsvc.sdk_analyzed_at IS NULL 
                    OR
                        (
                        (lsvc.analyze_result = 1 AND lsvc.sdk_analyzed_at < current_date - INTERVAL '180 days')
                        OR
                        (lsvc.analyze_result IN (2,3,4) AND lsvc.sdk_analyzed_at < current_date - INTERVAL '30 days')
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
                DISTINCT sa.id AS store_app,
                sa.store_id,
                sa.name,
                sa.installs,
                sa.rating_count,
                lsvc.analyze_result AS last_analyzed_result,
                lsvc.sdk_analyzed_at
            FROM
                user_requested_scan urs
            LEFT JOIN store_apps sa ON
                urs.store_id = sa.store_id
            LEFT JOIN latest_success_version_codes lsvc ON
                sa.id = lsvc.store_app
            RIGHT JOIN latest_version_codes lvc ON
                sa.id = lvc.store_app
            WHERE
                (lsvc.sdk_analyzed_at < urs.created_at
                    OR lsvc.sdk_analyzed_at IS NULL
                )
                AND (lvc.last_downloaded_at < current_date - INTERVAL '1 days'
                    OR lvc.last_downloaded_at IS NULL)
                AND sa.store = {store}
            )
        SELECT
            store_app,
            store_id,
            name,
            installs,
            rating_count,
            last_analyzed_result,
            sdk_analyzed_at
        FROM
            user_requested_apps_crawl
        UNION ALL
        SELECT
            store_app,
            store_id,
            name,
            installs,
            rating_count,
            last_analyzed_result,
            sdk_analyzed_at
        FROM
            scheduled_apps_crawl
        LIMIT {limit}
        ;
    """
    df = pd.read_sql(sel_query, con=database_connection.engine)
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


def get_local_stored_apks(database_connection: PostgresCon) -> pd.DataFrame:
    sel_query = """SELECT * FROM local_apks;"""
    df = pd.read_sql(sel_query, con=database_connection.engine)
    return df


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
