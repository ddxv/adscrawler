import datetime
import hashlib
import pathlib
from functools import lru_cache
from io import StringIO

import numpy as np
import pandas as pd
from psycopg import Connection
from psycopg.sql import SQL, Composed, Identifier
from sqlalchemy import bindparam, text
from sqlalchemy.sql.elements import TextClause

from adscrawler.config import CONFIG, SQL_DIR, get_logger
from adscrawler.dbcon.connection import PostgresCon

logger = get_logger(__name__)


def load_sql_file(file_name: str) -> TextClause:
    """Load local SQL file based on file name."""
    file_path = pathlib.Path(SQL_DIR, file_name)
    with file_path.open() as file:
        return text(file.read())


QUERY_APPS_TO_UPDATE_SECONDARY = load_sql_file("query_apps_to_update_secondary.sql")
QUERY_APPS_TO_UPDATE_PRIMARY = load_sql_file("query_apps_to_update_primary.sql")
QUERY_APPS_TO_UPDATE_ANY_NEW = load_sql_file("query_apps_to_update_any_new.sql")
QUERY_APPS_TO_DOWNLOAD = load_sql_file("query_apps_to_download.sql")
QUERY_APPS_TO_SDK_SCAN = load_sql_file("query_apps_to_sdk_scan.sql")
QUERY_APPS_TO_API_SCAN = load_sql_file("query_apps_to_api_scan.sql")
QUERY_API_CALLS_TO_CREATIVE_SCAN = load_sql_file("query_apps_to_creative_scan.sql")
QUERY_KEYWORDS_TO_CRAWL = load_sql_file("query_keywords_to_crawl.sql")
QUERY_APPS_MITM_IN_S3 = load_sql_file("query_apps_mitm_in_s3.sql")
QUERY_ZSCORES = load_sql_file("query_simplified_store_app_z_scores.sql")
QUERY_APPS_TO_PROCESS_KEYWORDS = load_sql_file("query_apps_to_process_keywords.sql")
QUERY_APPS_TO_PROCESS_METRICS = load_sql_file("query_apps_to_process_metrics.sql")


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

    insert_query = SQL(
        """
        INSERT INTO {table} ({columns})
        VALUES ({placeholders})
        {returning_clause}
    """
    ).format(
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


def prepare_for_psycopg(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col in df.select_dtypes(include=["datetimetz", "datetime64[ns]"]):
        # Convert to object dtype first so it can hold None
        # Note: This may be breaking in pandas3.0
        df[col] = (
            df[col]
            .apply(lambda x: x.to_pydatetime() if pd.notna(x) else None)
            .astype("object")
        )
    # Replace NaN (for floats, strings, etc.)
    df = df.astype(object).where(pd.notna(df), None)
    return df


def update_from_df(
    df: pd.DataFrame,
    table_name: str,
    database_connection: Connection,
    key_columns: list[str],
    update_columns: list[str],
    return_rows: bool = False,
    schema: str | None = None,
    md5_key_columns: list[str] | None = None,
    log: bool = False,
) -> pd.DataFrame | None:
    """Perform an UPDATE on a PostgreSQL table from a DataFrame.
    Parameters
    ----------
    df : pandas.DataFrame
        The DataFrame containing update data.
    table_name : str
        The name of the target table.
    database_connection : Connection
        The database connection object.
    key_columns : list of str
        Column name(s) on which to match for the UPDATE.
    update_columns : list of str
        Columns to update (excluding key columns).
    return_rows : bool, optional
        Whether to return the rows that were updated.
    schema : str, optional
        The name of the schema containing the target table.
    md5_key_columns: list of str, optional
        Key columns that use MD5 hashing in their index.
    log : bool, optional
        Print generated SQL statement for debugging.
    Returns
    -------
    pd.DataFrame or None
        DataFrame of updated rows if return_rows=True, else None.
    """
    raw_conn = database_connection.engine.raw_connection()
    # Handle special date columns
    if "crawled_date" in df.columns and df["crawled_date"].isna().all():
        df["crawled_date"] = pd.to_datetime(df["crawled_date"]).dt.date
        df["crawled_date"] = None
    if "release_date" in df.columns and df["release_date"].isna().all():
        df["release_date"] = None
    # Build table identifier
    table_identifier = Identifier(table_name)
    if schema:
        table_identifier = Composed([Identifier(schema), SQL("."), table_identifier])
    # Build UPDATE SET clause for update_columns only
    update_set = SQL(", ").join(
        SQL("{0} = %s").format(Identifier(col)) for col in update_columns
    )
    # Build WHERE conditions for key_columns
    if md5_key_columns:
        where_conditions = SQL(" AND ").join(
            (
                SQL("md5({col}) = %s").format(col=Identifier(col))
                if col in md5_key_columns
                else SQL("{col} = %s").format(col=Identifier(col))
            )
            for col in key_columns
        )
    else:
        where_conditions = SQL(" AND ").join(
            SQL("{col} = %s").format(col=Identifier(col)) for col in key_columns
        )
    if return_rows:
        update_query = SQL(
            """
            UPDATE {table}
            SET {update_set}
            WHERE {where_conditions}
            RETURNING *
            """
        ).format(
            table=table_identifier,
            update_set=update_set,
            where_conditions=where_conditions,
        )
    else:
        update_query = SQL(
            """
            UPDATE {table}
            SET {update_set}
            WHERE {where_conditions}
            """
        ).format(
            table=table_identifier,
            update_set=update_set,
            where_conditions=where_conditions,
        )
    if log:
        logger.info(f"Update query: {update_query.as_string(raw_conn)}")
    all_columns = update_columns + key_columns
    with raw_conn.cursor() as cur:
        # Prepare data
        data = [
            tuple(row) for row in df[all_columns].itertuples(index=False, name=None)
        ]
        if log:
            logger.info(f"Update data sample: {data[:5] if len(data) > 5 else data}")
        # Execute updates
        if return_rows:
            all_results = []
            for row in data:
                cur.execute(update_query, row)
                result = cur.fetchall()
                all_results.extend(result)
            if all_results:
                column_names = [desc[0] for desc in cur.description]
                return_df = pd.DataFrame(all_results, columns=column_names)
            else:
                return_df = pd.DataFrame()
        else:
            cur.executemany(update_query, data)
            return_df = None
    raw_conn.commit()
    return return_df


def upsert_bulk(
    df: pd.DataFrame,
    table_name: str,
    database_connection: Connection,
    key_columns: list[str],
) -> None:
    temp_table = f"temp_{table_name}"
    with database_connection.engine.begin() as conn:
        conn.execute(
            text(
                f"CREATE TEMP TABLE {temp_table} "
                f"(LIKE {table_name} INCLUDING DEFAULTS) ON COMMIT DROP"
            )
        )
        buffer = StringIO()
        df.to_csv(buffer, index=False, header=False)
        buffer.seek(0)
        raw = conn.connection
        with raw.cursor() as cur:
            with cur.copy(
                f"COPY {temp_table} ({', '.join(df.columns)}) FROM STDIN WITH CSV"
            ) as copy:
                copy.write(buffer.getvalue())
            # cur.execute(f"ANALYZE {temp_table}")
        all_cols = [f'"{c}"' for c in df.columns]
        update_cols = [
            f'"{c}" = EXCLUDED."{c}"' for c in df.columns if c not in key_columns
        ]
        where_clause = " OR ".join(
            f'{table_name}."{c}" IS DISTINCT FROM EXCLUDED."{c}"'
            for c in df.columns
            if c not in key_columns
        )
        query = f"""
            INSERT INTO {table_name} ({", ".join(all_cols)})
            SELECT {", ".join(all_cols)} FROM {temp_table}
            ON CONFLICT ({", ".join(f'"{c}"' for c in key_columns)})
            DO UPDATE SET {", ".join(update_cols)}
            WHERE {where_clause}
        """
        conn.execute(text(query))


def upsert_df(
    df: pd.DataFrame,
    table_name: str,
    database_connection: Connection,
    key_columns: list[str],
    insert_columns: list[str],
    return_rows: bool = False,
    schema: str | None = None,
    md5_key_columns: list[str] | None = None,
    on_conflict_update: bool = True,
    log: bool = False,
) -> pd.DataFrame | None:
    """Perform an "upsert" on a PostgreSQL table from a DataFrame.
    Constructs an INSERT … ON CONFLICT statement, uploads the DataFrame to a
    temporary table, and then executes the INSERT.

    Parameters
    ----------
    df : pandas.DataFrame
        The DataFrame to be upserted.
    table_name : str
        The name of the target table.
    database_connection : Connection
        The SQLAlchemy Engine to use.
    schema : str, optional
        The name of the schema containing the target table.
    key_columns : list of str, optional
        A list of the column name(s) on which to match. If omitted, the
        primary key columns of the target table will be used.
    insert_columns : list of str, all columns to insert/update
    return_rows : bool, optional
        Whether to return the rows that were inserted/updated, usually to get db ids
    md5_key_columns: list of str columns (usually >1000 chars needs this)
        that have Postgresql md5() used in the UNIQUE INDEX
        allows upsert without hitting index size limits
    on_conflict_update: bool, optional
        Whether to update the existing rows on conflict, default True
    log : bool, optional
        Print generated SQL statement for debugging.
    """

    # Validate parameters
    if not on_conflict_update and return_rows:
        raise ValueError(
            "return_rows=True cannot be used with on_conflict_update=False "
            "because DO NOTHING doesn't guarantee the returned rows were actually inserted"
        )

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
    if md5_key_columns:
        non_md5_key_columns = [x for x in key_columns if x not in md5_key_columns]
        conflict_columns = SQL(" , ").join(
            [SQL("md5(") + Identifier(col) + SQL(")") for col in md5_key_columns]
            + [Identifier(col) for col in non_md5_key_columns]
        )
    else:
        conflict_columns = SQL(", ").join(map(Identifier, key_columns))

    if on_conflict_update:
        update_set = SQL(", ").join(
            SQL("{0} = EXCLUDED.{0}").format(Identifier(col)) for col in all_columns
        )
        action_clause = SQL("DO UPDATE SET {update_set}").format(update_set=update_set)
    else:
        action_clause = SQL("DO NOTHING")

    # Upsert query without RETURNING clause
    upsert_query = SQL(
        """
        INSERT INTO {table} ({columns})
        VALUES ({placeholders})
        ON CONFLICT ({conflict_columns})
        {action_clause}
    """
    ).format(
        table=table_identifier,
        columns=columns,
        placeholders=placeholders,
        conflict_columns=conflict_columns,
        action_clause=action_clause,
    )

    sel_where_conditions = SQL(" AND ").join(
        (
            SQL("md5({}) = ANY(%s::text[])").format(Identifier(col))
            if md5_key_columns and col in md5_key_columns
            else SQL("{} = ANY(%s)").format(Identifier(col))
        )
        for col in key_columns
    )

    select_query = SQL(
        """
        SELECT * FROM {table}
        WHERE {where_conditions}
    """
    ).format(table=table_identifier, where_conditions=sel_where_conditions)
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
            if len(key_columns) == 1 and key_columns[0] in (md5_key_columns or []):
                md5_values = [
                    (
                        hashlib.md5(v.encode("utf-8")).hexdigest()
                        if v is not None
                        else None
                    )
                    for v in df[key_columns[0]].tolist()
                ]
                where_values = [md5_values]
            else:
                where_values = [df[col].tolist() for col in key_columns]
            cur.execute(select_query, where_values)
            result = cur.fetchall()
            column_names = [desc[0] for desc in cur.description]
            return_df = pd.DataFrame(result, columns=column_names)
        else:
            return_df = None

    raw_conn.commit()
    return return_df


def clean_app_ranks_weekly_table(database_connection: PostgresCon) -> None:
    # Use a smaller limit to prevent long locks
    batch_size = 100000
    del_query = f"""
        DELETE FROM frontend.store_app_ranks_weekly
        WHERE ctid IN (
            SELECT ctid FROM frontend.store_app_ranks_weekly
            WHERE crawled_date < CURRENT_DATE - INTERVAL '14 days'
              AND EXTRACT(DOW FROM crawled_date) != 1
            LIMIT {batch_size}
        );
    """
    raw_conn = database_connection.engine.raw_connection()
    # Ensure we aren't in an implicit transaction block that stays open
    raw_conn.set_session(autocommit=True)
    try:
        with raw_conn.cursor() as cur:
            while True:
                cur.execute(del_query)
                rows_affected = cur.rowcount
                print(f"Deleted {rows_affected} rows...")

                if rows_affected == 0:
                    break
    finally:
        raw_conn.close()


def delete_and_insert(
    df: pd.DataFrame,
    table_name: str,
    schema: str | None,
    database_connection: Connection,
    insert_columns: list[str],
    delete_by_keys: list[str],
    delete_keys_have_duplicates: bool = False,
) -> pd.DataFrame | None:
    """Replace rows in a table by deleting on key values then inserting the DataFrame."""
    if df.empty:
        return None

    keys_df = df[delete_by_keys].drop_duplicates().reset_index(drop=True)
    if not delete_keys_have_duplicates:
        assert len(keys_df) == len(df), "Duplicate keys found in df"

    keys_df = prepare_for_psycopg(keys_df)

    raw_conn = database_connection.engine.raw_connection()
    table_identifier = Identifier(table_name)
    if schema:
        table_identifier = Composed([Identifier(schema), SQL("."), table_identifier])

    where_conditions = SQL(" AND ").join(
        SQL("{col} = %s").format(col=Identifier(col)) for col in delete_by_keys
    )
    delete_query = SQL(
        """
        DELETE FROM {table}
        WHERE {where_conditions}
        """
    ).format(table=table_identifier, where_conditions=where_conditions)

    delete_data = [tuple(row) for row in keys_df.itertuples(index=False, name=None)]

    with raw_conn.cursor() as cur:
        if delete_data:
            cur.executemany(delete_query, delete_data)
    raw_conn.commit()
    raw_conn.close()

    return insert_df(
        df=df,
        insert_columns=insert_columns,
        table_name=table_name,
        database_connection=database_connection,
        schema=schema,
    )


@lru_cache(maxsize=1)
def query_all_developers(database_connection: PostgresCon) -> pd.DataFrame:
    """Query all developers from the database."""
    sel_query = """SELECT 
     id, store, developer_id
     FROM developers
     ;
    """
    df = pd.read_sql(sel_query, database_connection.engine)
    return df


def query_developers(
    database_connection: PostgresCon,
    store: int,
    limit: int = 1000,
) -> pd.DataFrame:
    logger.info(f"Query developers {store=} start")
    before_date = (datetime.datetime.today() - datetime.timedelta(days=15)).strftime(
        "%Y-%m-%d",
    )
    sel_query = f"""
    SELECT 
            d.*,
            SUM(agm.installs) AS total_installs,
            dc.apps_crawled_at
        FROM
            developers d
        LEFT JOIN logging.developers_crawled_at dc
            ON d.id = dc.developer
        LEFT JOIN store_apps sa 
            ON d.id = sa.developer 
        LEFT JOIN app_global_metrics_latest agm
            ON sa.id = agm.store_app
        WHERE d.store = {store} 
            AND (apps_crawled_at <= '{before_date}' OR apps_crawled_at IS NULL)
            AND sa.crawl_result = 1
        GROUP BY
            d.id, dc.apps_crawled_at
        ORDER BY apps_crawled_at::date NULLS FIRST,
        total_installs DESC NULLS LAST
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
    df["version_code"] = df["version_code"].fillna("-1")
    df = df[insert_columns]
    df.to_sql(
        name="store_app_downloads",
        schema="logging",
        con=database_connection.engine,
        if_exists="append",
        index=False,
    )


def log_creative_scan_results(
    df: pd.DataFrame, database_connection: PostgresCon
) -> None:
    my_columns = [
        "url",
        "tld_url",
        "path",
        "content_type",
        "run_id",
        "pub_store_id",
        "file_extension",
        "creative_size",
        "error_msg",
    ]
    insert_columns = [x for x in my_columns if x in df.columns]
    df = df[insert_columns]
    df.to_sql(
        name="creative_scan_results",
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


def query_latest_api_scan_by_store_id(
    store_ids: list[str], database_connection: PostgresCon
) -> pd.DataFrame:
    store_ids_str = "'" + "','".join(store_ids) + "'"
    sel_query = text(
        f"""WITH last_successful_scanned AS (
            SELECT DISTINCT ON
            (vc.store_app) vc.version_code, vc.store_app, vasr.id as run_id
            FROM
                version_code_api_scan_results AS vasr
            LEFT JOIN version_codes AS vc
                ON vasr.version_code_id = vc.id
            WHERE
                vc.updated_at >= '2025-04-20'
            ORDER BY vc.store_app ASC, vasr.run_at DESC
            )
            SELECT lss.*, sa.store_id FROM last_successful_scanned lss
            left join store_apps sa
                on lss.store_app = sa.id
            where sa.store_id IN ({store_ids_str})
            ;
    """
    )
    df = pd.read_sql(sel_query, database_connection.engine)
    return df


def upser_sdk_details_df(
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


@lru_cache(maxsize=2)
def query_store_id_map_cached(
    database_connection: PostgresCon,
    store: int | None,
) -> pd.DataFrame:
    where_statement = ""
    if store:
        where_statement += f"WHERE store = {store} "
    sel_query = f"""SELECT
        id, store, store_id
        FROM
        store_apps
        {where_statement}
        ;
        """
    df = pd.read_sql(sel_query, database_connection.engine)
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


@lru_cache(maxsize=1)
def query_countries(database_connection: PostgresCon) -> pd.DataFrame:
    sel_query = """SELECT
        c.*, t.tier_slug as tier
        FROM
        countries c
        LEFT JOIN public.tiers t on c.tier_id = t.id
        ;
        """
    df = pd.read_sql(sel_query, database_connection.engine)
    return df


@lru_cache(maxsize=1)
def query_companies(database_connection: PostgresCon) -> pd.DataFrame:
    sel_query = """SELECT
        c.id as company_id,
        c.name as company_name,
        c.parent_company_id as parent_company_id,
        c.logo_url as company_logo_url,
        c.linkedin_url as company_linkedin_url,
        ad.id as company_domain_id,
        ad.domain_name as company_domain
        FROM
        adtech.companies c
        left join domains ad on c.domain_id = ad.id
        where c.id > 0
        ;
        """
    df = pd.read_sql(sel_query, database_connection.engine)
    return df


def update_company_logo_url(
    company_id: int, logo_url: str, database_connection: PostgresCon
) -> None:
    update_query = """UPDATE adtech.companies SET logo_url = %s WHERE id = %s"""
    with database_connection.get_cursor() as cur:
        cur.execute(update_query, (logo_url, company_id))


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


def query_pub_domains_to_crawl_ads_txt(
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
        exclude_str = (
            f"AND (pdcr.crawled_at <= '{before_date}' OR pdcr.crawled_at IS NULL)"
        )
    if limit:
        limit_str = f"LIMIT {limit}"
    sel_query = f"""SELECT
            DISTINCT pd.id, pd.domain_name as url, pdcr.crawled_at
        FROM
            app_urls_map aum
        LEFT JOIN domains pd ON
            pd.id = aum.pub_domain
        LEFT JOIN adstxt_crawl_results pdcr on (pd.id = pdcr.domain_id)
        LEFT JOIN store_apps sa ON
            sa.id = aum.store_app
        WHERE
            sa.ad_supported
            AND sa.crawl_result = 1
            {exclude_str}
        ORDER BY
            pdcr.crawled_at NULLS FIRST
        {limit_str}
        ; 
        """
    df = pd.read_sql(sel_query, database_connection.engine)
    return df


@lru_cache(maxsize=1)
def query_urls_hash_map_cached(database_connection: PostgresCon) -> pd.DataFrame:
    """
    Get URL IDs and hashes from the urls table.
    Returns DataFrame with columns: url_id, url
    """
    sel_query = """SELECT id, url_hash FROM adtech.urls"""
    df = pd.read_sql(sel_query, database_connection.engine)
    df = df.rename(columns={"id": "url_id"})
    return df


def query_urls_by_hashes(
    hashes: list[str], database_connection: PostgresCon
) -> pd.DataFrame:
    hashes_tuple = tuple(hashes)
    if not hashes_tuple:
        return pd.DataFrame(columns=["id", "url_hash"])
    return _query_urls_by_hashes_cached(hashes_tuple, database_connection)


@lru_cache(maxsize=1000)
def _query_urls_by_hashes_cached(
    hashes: tuple[str, ...], database_connection: PostgresCon
) -> pd.DataFrame:
    sel_query = text(
        """SELECT id, url_hash FROM adtech.urls WHERE url_hash IN :hashes"""
    ).bindparams(bindparam("hashes", expanding=True))
    df = pd.read_sql(sel_query, database_connection.engine, params={"hashes": hashes})
    return df


@lru_cache(maxsize=1000)
def get_click_url_redirect_chains(
    run_id: int, database_connection: PostgresCon
) -> pd.DataFrame:
    sel_query = f"""SELECT
        urc.api_call_id,
        urc.hop_index,
        urc.is_chain_start,
        urc.is_chain_end,
        u.url,
        ur.url AS redirect_url
    FROM
        adtech.url_redirect_chains urc
    LEFT JOIN adtech.urls u ON urc.url_id = u.id
    LEFT JOIN adtech.urls ur ON urc.next_url_id = ur.id
    WHERE
        urc.run_id = {run_id}
    """
    df = pd.read_sql(sel_query, database_connection.engine)
    return df


def delete_app_url_mapping(app_url_id: int, database_connection: PostgresCon) -> None:
    del_query = "DELETE FROM app_urls_map WHERE id = %s"
    logger.info(f"{app_url_id=} delete app_urls_map start")
    try:
        with database_connection.get_cursor() as cur:
            cur.execute(del_query, (app_url_id,))
        logger.info(f"{app_url_id=} delete app_urls_map completed successfully")
    except Exception as e:
        logger.error(f"{app_url_id=} delete app_urls_map failed: {e}")
        raise


@lru_cache(maxsize=1)
def get_store_app_columns(database_connection: PostgresCon) -> list[str]:
    sel_query = """SELECT * FROM store_apps LIMIT 1"""
    df = pd.read_sql(sel_query, database_connection.engine)
    columns = df.columns.tolist()
    # Auto generated columns
    columns = [
        x
        for x in columns
        if x not in ["id", "updated_at", "created_at", "textsearchable_index_col"]
    ]
    return columns


def check_mv_exists(database_connection: PostgresCon, mv_name: str) -> bool:
    query = f"""
        SELECT relispopulated
        FROM pg_class
        WHERE relname = '{mv_name}';
    """
    with database_connection.get_cursor() as cur:
        cur.execute(query)
        row = cur.fetchone()
    return row[0] if row is not None else False


def get_crawl_scenario_countries(
    database_connection: PostgresCon, scenario_name: str
) -> pd.DataFrame:
    query = f"""SELECT c.alpha2 as country_code, cc.priority
        FROM public.crawl_scenario_country_config cc
        JOIN public.countries c ON cc.country_id = c.id
        JOIN public.crawl_scenarios s ON cc.scenario_id = s.id
        WHERE s.name = '{scenario_name}'
          AND cc.enabled = true
        ORDER BY cc.priority;
    """
    df = pd.read_sql(query, database_connection.engine)
    return df


def query_store_apps_to_update(
    database_connection: PostgresCon,
    store: int,
    country_priority_group: int,
    log_query: bool = False,
    limit: int = 1000,
) -> pd.DataFrame:
    short_update_days = CONFIG["crawl-settings"].get("short_update_days", 1)
    short_update_installs = CONFIG["crawl-settings"].get("short_update_installs", 1000)
    short_update_ratings = CONFIG["crawl-settings"].get("short_update_ratings", 100)
    long_update_days = CONFIG["crawl-settings"].get("long_update_days", 2)
    max_recrawl_days = CONFIG["crawl-settings"].get("max_recrawl_days", 15)
    short_update_ts = datetime.datetime.now(tz=datetime.UTC) - datetime.timedelta(
        days=short_update_days
    )
    long_update_ts = datetime.datetime.now(tz=datetime.UTC) - datetime.timedelta(
        days=long_update_days
    )
    max_recrawl_ts = datetime.datetime.now(tz=datetime.UTC) - datetime.timedelta(
        days=max_recrawl_days
    )
    year_ago_ts = datetime.datetime.now(tz=datetime.UTC) - datetime.timedelta(days=365)
    params = {
        "store": store,
        "country_crawl_priority": country_priority_group,
        "short_update_ts": short_update_ts,
        "short_update_installs": short_update_installs,
        "short_update_ratings": short_update_ratings,
        "long_update_ts": long_update_ts,
        "max_recrawl_ts": max_recrawl_ts,
        "year_ago_ts": year_ago_ts,
        "mylimit": limit,
    }
    country_priority_group_query = {
        -1: QUERY_APPS_TO_UPDATE_ANY_NEW,
        1: QUERY_APPS_TO_UPDATE_PRIMARY,
        2: QUERY_APPS_TO_UPDATE_SECONDARY,
    }
    query = country_priority_group_query[country_priority_group]

    if log_query:
        # Compile and print the query with parameters
        compiled_query = query.bindparams(**params).compile(
            database_connection.engine, compile_kwargs={"literal_binds": True}
        )
        logger.info(f"Executing query:\n{compiled_query}")
    df = pd.read_sql(
        query,
        con=database_connection.engine,
        params=params,
        dtype={"store_app": int, "store": int, "store_id": str},
    )
    df["language"] = "en"
    return df


def query_keywords_to_crawl(
    database_connection: PostgresCon,
    limit: int,
) -> pd.DataFrame:
    df = pd.read_sql(
        QUERY_KEYWORDS_TO_CRAWL,
        params={"mylimit": limit},
        con=database_connection.engine,
    )
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
) -> pd.DataFrame:
    df = pd.read_sql(
        QUERY_APPS_TO_DOWNLOAD,
        con=database_connection.engine,
        params={"store": store},
    )
    return df


def query_zscores(database_connection: PostgresCon, target_week: str) -> pd.DataFrame:
    df = pd.read_sql(
        QUERY_ZSCORES,
        con=database_connection.engine,
        params={"target_week": target_week},
    )
    return df


@lru_cache(maxsize=1)
def query_sdk_keys(database_connection: PostgresCon) -> pd.DataFrame:
    sel_query = """SELECT * FROM ad_network_sdk_keys;"""
    df = pd.read_sql(
        sel_query,
        con=database_connection.engine,
    )
    return df


def query_apps_to_sdk_scan(
    database_connection: PostgresCon,
    store: int,
) -> pd.DataFrame:
    df = pd.read_sql(
        QUERY_APPS_TO_SDK_SCAN,
        con=database_connection.engine,
        params={"store": store},
    )
    return df


def query_all_apps_to_process(
    database_connection: PostgresCon,
) -> None:
    download_df = query_apps_to_download(
        database_connection=database_connection, store=1
    )
    sdk_df = query_apps_to_sdk_scan(database_connection=database_connection, store=1)
    api_df = query_apps_to_api_scan(database_connection=database_connection, store=1)

    ipa_download_df = query_apps_to_download(
        database_connection=database_connection, store=2
    )
    ipa_sdk_df = query_apps_to_sdk_scan(
        database_connection=database_connection, store=2
    )

    android_downloads = download_df.shape[0]
    android_sdks = sdk_df.shape[0]
    android_apis = api_df.shape[0]

    ipa_downloads = ipa_download_df.shape[0]
    ipa_sdks = ipa_sdk_df.shape[0]

    logger.info(f"Android: {android_downloads} -> {android_sdks} -> {android_apis}")
    logger.info(f"iOS: {ipa_downloads} -> {ipa_sdks}")
    return


def query_apps_to_api_scan(
    database_connection: PostgresCon, store: int
) -> pd.DataFrame:
    df = pd.read_sql(
        QUERY_APPS_TO_API_SCAN,
        con=database_connection.engine,
        params={"store": store},
    )
    return df


def query_apps_to_process_keywords(
    database_connection: PostgresCon, limit: int = 10000
) -> pd.DataFrame:
    """Query apps to process keywords."""
    df = pd.read_sql(
        QUERY_APPS_TO_PROCESS_KEYWORDS,
        con=database_connection.engine,
        params={"mylimit": limit},
    )
    return df


def query_apps_to_process_global_metrics(
    database_connection: PostgresCon, batch_size: int = 10000
) -> pd.DataFrame:
    """Query apps to process metrics."""
    df = pd.read_sql(
        QUERY_APPS_TO_PROCESS_METRICS,
        con=database_connection.engine,
        params={"batch_size": batch_size},
    )
    tiers = ["tier1_pct", "tier2_pct", "tier3_pct"]
    df[tiers] = (df[tiers] / 10000).fillna(0)
    return df


def query_apps_mitm_in_s3(database_connection: PostgresCon) -> pd.DataFrame:
    df = pd.read_sql(
        QUERY_APPS_MITM_IN_S3,
        con=database_connection.engine,
    )
    return df


@lru_cache(maxsize=2)
def query_api_calls_to_creative_scan(
    database_connection: PostgresCon, recent_months: bool = False
) -> pd.DataFrame:
    earliest_date = (datetime.datetime.now() - datetime.timedelta(days=365)).strftime(
        "%Y-%m-%d"
    )
    if recent_months:
        earliest_date = (
            datetime.datetime.now() - datetime.timedelta(days=60)
        ).strftime("%Y-%m-%d")
    df = pd.read_sql(
        QUERY_API_CALLS_TO_CREATIVE_SCAN,
        con=database_connection.engine,
        params={"earliest_date": earliest_date},
    )
    return df


@lru_cache(maxsize=1)
def query_creative_assets(database_connection: PostgresCon) -> pd.DataFrame:
    sel_query = """SELECT
        *
        FROM
        creative_assets
        ;
        """
    df = pd.read_sql(sel_query, con=database_connection.engine)
    return df


def query_creative_records(database_connection: PostgresCon) -> pd.DataFrame:
    df = pd.read_sql(
        """SELECT 
          DISTINCT 
          pub_store_id, csr.run_id 
        FROM 
        logging.creative_scan_results csr""",
        con=database_connection.engine,
    )
    return df


def query_all_store_app_descriptions(
    language_slug: str,
    database_connection: PostgresCon,
) -> pd.DataFrame:
    sel_query = f"""SELECT
    DISTINCT ON (store_app)
        description
    FROM
        store_apps_descriptions sad
    LEFT JOIN languages l ON sad.language_id = l.id
    WHERE l.language_slug = '{language_slug}'
        ORDER BY store_app, updated_at desc
    ;
    """
    df = pd.read_sql(sel_query, con=database_connection.engine)
    return df


@lru_cache(maxsize=1)
def query_all_domains(database_connection: PostgresCon) -> pd.DataFrame:
    sel_query = """SELECT
        *
        FROM
        domains
        ;
        """
    df = pd.read_sql(sel_query, con=database_connection.engine)
    return df


@lru_cache(maxsize=1)
def query_ad_domains(database_connection: PostgresCon) -> pd.DataFrame:
    sel_query = """WITH all_ad_domains AS (
             SELECT DISTINCT d.id AS domain_id, ad_domain AS domain_name 
             FROM adtech.combined_store_apps_companies csac
             LEFT JOIN domains d ON csac.ad_domain = d.domain_name
             WHERE ad_domain IS NOT null
             UNION
             SELECT DISTINCT cdm.domain_id, d.domain_name FROM adtech.company_domain_mapping cdm
             LEFT JOIN domains d ON cdm.domain_id = d.id
             --avoid including special company 'domains'
             WHERE cdm.company_id > 0
             )
             SELECT domain_id, domain_name FROM all_ad_domains
             ;
             """
    df = pd.read_sql(sel_query, con=database_connection.engine)
    return df


@lru_cache(maxsize=1)
def query_keywords_base(database_connection: PostgresCon) -> pd.DataFrame:
    sel_query = """SELECT
    k.id as keyword_id, k.keyword_text
    FROM
    keywords_base kb
    LEFT JOIN keywords k ON
        k.id = kb.keyword_id
    ;
    """
    df = pd.read_sql(sel_query, con=database_connection.engine)
    return df


@lru_cache(maxsize=1000)
def query_store_app_by_store_id_cached(
    database_connection: PostgresCon,
    store_id: str,
    case_insensitive: bool = False,
) -> int:
    return query_store_app_by_store_id(
        database_connection=database_connection,
        store_id=store_id,
        case_insensitive=case_insensitive,
    )


def query_store_app_by_store_id(
    database_connection: PostgresCon,
    store_id: str,
    case_insensitive: bool = False,
) -> int:
    if case_insensitive:
        sel_query = f"""SELECT * FROM store_apps WHERE store_id ILIKE '{store_id}'"""
    else:
        sel_query = f"""SELECT * FROM store_apps WHERE store_id = '{store_id}'"""
    df = pd.read_sql(sel_query, con=database_connection.engine)
    if df.empty:
        raise ValueError(f"Store id {store_id} not found")
    try:
        return int(df.iloc[0]["id"])
    except Exception:
        logger.exception(f"Error getting store app id for '{store_id}'")
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
        df = insert_version_code(
            version_str=version_code,
            store_app=store_app,
            crawl_result=4,
            database_connection=database_connection,
            return_rows=True,
        )
        return None
    try:
        return int(df.iloc[0]["id"])
    except Exception:
        logger.exception(
            f"Error getting version code id for {store_app} and {version_code}"
        )
        raise


def get_failed_mitm_logs(database_connection: PostgresCon) -> pd.DataFrame:
    sel_query = """WITH last_run_result AS (SELECT DISTINCT ON (run_id)
      run_id, pub_store_id, error_msg, inserted_at
        FROM logging.creative_scan_results 
      ORDER BY run_id, inserted_at DESC
      )
      SELECT * 
          FROM last_run_result
          WHERE error_msg like 'CRITICAL %%'
        ;
    """
    df = pd.read_sql(sel_query, con=database_connection.engine)
    return df


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


@lru_cache(maxsize=1000)
def query_api_call_id_for_uuid(mitm_uuid: str, database_connection: PostgresCon) -> int:
    api_calls = query_api_calls_id_uuid_map(database_connection)
    filtered_df = api_calls[api_calls["mitm_uuid"] == mitm_uuid]
    assert filtered_df.shape[0] == 1, "Failed to find api_call_id for mitm_uuid"
    api_call_id: int = filtered_df["api_call_id"].to_numpy()[0]
    return api_call_id


@lru_cache(maxsize=1)
def query_api_calls_id_uuid_map(database_connection: PostgresCon) -> pd.DataFrame:
    sel_query = """SELECT id, mitm_uuid FROM api_calls"""
    df = pd.read_sql(sel_query, con=database_connection.engine)
    df["mitm_uuid"] = df["mitm_uuid"].astype(str)
    df = df.rename(columns={"id": "api_call_id"})
    return df


def query_api_calls_for_mitm_uuids(
    database_connection: PostgresCon, mitm_uuids: list[str]
) -> pd.DataFrame:
    api_calls = query_api_calls_id_uuid_map(database_connection)
    filtered_df = api_calls[api_calls["mitm_uuid"].isin(mitm_uuids)]
    return filtered_df


@lru_cache(maxsize=1)
def get_all_mmp_tlds(database_connection: PostgresCon) -> pd.DataFrame:
    sel_query = """SELECT
                c.id,
                name,
                ad.domain_name AS mmp_tld
            FROM
                adtech.companies c
            LEFT JOIN adtech.company_categories cc ON
                c.id = cc.company_id
            LEFT JOIN adtech.company_domain_mapping cdm ON
                c.id = cdm.company_id
            LEFT JOIN domains ad ON
                cdm.domain_id = ad.id
            WHERE
                cc.category_id = 2
                AND c.id != -2
            ;
            """
    df = pd.read_sql(sel_query, con=database_connection.engine)
    return df


def get_latest_app_country_history(
    database_connection: PostgresCon,
    snapshot_date: datetime.date,
    store_app_ids: list,
    days_back: int,
    chunk_size: int,
    store: int,
) -> pd.DataFrame:
    chunks = [
        store_app_ids[i : i + chunk_size]
        for i in range(0, len(store_app_ids), chunk_size)
    ]
    log_info = f"query app_country_metrics_history latest apps:{len(store_app_ids)}"
    logger.info(f"{log_info} start days:{days_back} chunks:{len(chunks)}")
    end_date = snapshot_date.strftime("%Y-%m-%d")
    results = []
    if store == 1:
        metric_cols = "review_count"
    elif store == 2:
        metric_cols = "rating, rating_count, one_star, two_star, three_star, four_star, five_star, installs_est"
    for _i, chunk_ids in enumerate(chunks):
        id_list_str = ",".join(map(str, chunk_ids))
        if days_back == 1:
            sel_query = f"""
            SELECT
                acmh.store_app,
                acmh.snapshot_date,
                acmh.country_id,
                {metric_cols}
            FROM app_country_metrics_history acmh
            WHERE acmh.store_app IN ({id_list_str})
              AND acmh.snapshot_date = '{end_date}'
            """
        else:
            start_date = (snapshot_date - datetime.timedelta(days=days_back)).strftime(
                "%Y-%m-%d"
            )
            sel_query = f"""
            SELECT DISTINCT ON (acmh.store_app, acmh.country_id)
                acmh.store_app,
                acmh.snapshot_date,
                acmh.country_id,
                {metric_cols}
            FROM app_country_metrics_history acmh
            WHERE acmh.store_app IN ({id_list_str})
              AND acmh.snapshot_date >= '{start_date}'
              AND acmh.snapshot_date <= '{end_date}'
            ORDER BY
                store_app,
                country_id,
                snapshot_date DESC
        """
        # Pull chunk and append to results list
        chunk_df = pd.read_sql(sel_query, con=database_connection.engine)
        results.append(chunk_df)
    if not results:
        return pd.DataFrame()
    country_map = query_countries(database_connection)
    df = pd.concat(results, ignore_index=True)
    df = pd.merge(
        df, country_map[["id", "tier"]], how="left", left_on="country_id", right_on="id"
    )
    df["crawled_date"] = pd.to_datetime(df["snapshot_date"])
    df = df.drop(columns=["snapshot_date"])
    logger.info(f"{log_info} returning {len(df.shape)} rows")
    return df


def get_retention_benchmarks(database_connection: PostgresCon) -> pd.DataFrame:
    sel_query = """WITH 
         retention_benchmarks AS (
             SELECT
                 mac.store,
                 mac.category AS app_category,
                 -- Retention D1 Logic
             COALESCE(rgb.d1, 
                 CASE 
                     WHEN mac.category LIKE 'game%%' THEN (SELECT d1 FROM retention_global_benchmarks WHERE app_category = 'games' LIMIT 1)
                     ELSE (SELECT d1 FROM retention_global_benchmarks WHERE app_category = 'apps' LIMIT 1)
                 END
             ) AS d1,
                 -- Retention D7 Logic
             COALESCE(rgb.d7, 
                 CASE 
                     WHEN mac.category LIKE 'game%%' THEN (SELECT d7 FROM retention_global_benchmarks WHERE app_category = 'games' LIMIT 1)
                     ELSE (SELECT d7 FROM retention_global_benchmarks WHERE app_category = 'apps' LIMIT 1)
                 END
             ) AS d7,
                 -- Retention D30 Logic
             COALESCE(rgb.d30, 
                 CASE 
                     WHEN mac.category LIKE 'game%%' THEN (SELECT d30 FROM retention_global_benchmarks WHERE app_category = 'games' LIMIT 1)
                     ELSE (SELECT d30 FROM retention_global_benchmarks WHERE app_category = 'apps' LIMIT 1)
                 END
             ) AS d30
             FROM
                 mv_app_categories mac
             LEFT JOIN retention_global_benchmarks rgb 
             ON
                 mac.category = rgb.app_category
                 AND mac.store = rgb.store
         )
         SELECT
             store,
             app_category,
             d1,
             d7,
             d30
         FROM
             retention_benchmarks
    ;
    """
    df = pd.read_sql(sel_query, con=database_connection.engine)
    return df


def get_ecpm_benchmarks(database_connection: PostgresCon) -> pd.DataFrame:
    sel_query = """SELECT 
        store, tier_slug, af."name" ad_format, ecpm 
    FROM public.ecpm_benchmarks eb
    LEFT JOIN tiers t ON eb.tier_id = t.id
    LEFT JOIN adtech.ad_formats af ON eb.ad_format_id = af.id
        ;
    """
    df = pd.read_sql(sel_query, con=database_connection.engine)
    return df
