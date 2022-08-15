import pandas as pd
from dbcon.connection import PostgresCon
from config import get_logger
import numpy as np

logger = get_logger(__name__)


def upsert_df(
    table_name,
    insert_columns,
    df,
    key_columns,
    database_connection: PostgresCon,
    log=None,
    return_rows: bool = False,
):
    db_cols_str = ", ".join([f'"{col}"' for col in insert_columns])
    key_cols_str = ", ".join([f'"{col}"' for col in key_columns])
    values_str = ", ".join([f"%({col})s" for col in insert_columns])
    set_update = ", ".join([f"{col} = excluded.{col}" for col in insert_columns])
    if isinstance(df, pd.Series):
        df = pd.DataFrame(df).T
    for col in insert_columns:
        if (
            pd.api.types.is_string_dtype(df[col])
            and pd.api.types.infer_dtype(df[col]) == "string"
        ):
            df[col] = df[col].apply(lambda x: x.replace("'", "''"))
    if return_rows:
        return_str = " RETURNING * "
    else:
        return_str = ""
    insert_query = f""" 
        INSERT INTO {table_name} ({db_cols_str})
        VALUES ({values_str})
        ON CONFLICT ({key_cols_str})
        DO UPDATE SET {set_update}
        {return_str}
        ;
        """
    values = df[insert_columns].to_dict("records")
    if log:
        logger.info(f"MY INSERT QUERY: {insert_query.format(values)}")
    with database_connection.engine.begin() as connection:
        result = connection.execute(insert_query, values)
    return result


def insert_get(
    table_name: str,
    df: pd.DataFrame | pd.Series,
    insert_columns: str | list[str],
    key_columns: str | list[str],
    database_connection: PostgresCon,
    log: bool | None = None,
) -> pd.DataFrame:
    logger.info(f"insert_get table: {table_name}")

    if isinstance(insert_columns, str):
        insert_columns = [insert_columns]
    if isinstance(key_columns, str):
        key_columns = [key_columns]
    if isinstance(df, pd.Series):
        df = pd.DataFrame(df).T
    result = upsert_df(
        table_name,
        insert_columns,
        df,
        key_columns,
        database_connection=database_connection,
        return_rows=True,
    )
    logger.info(f"Upserted rows: {result.rowcount}")
    if result.returns_rows:
        get_df = pd.DataFrame(result.fetchall())
    else:
        get_df = pd.DataFrame()
    logger.info(f"Returning df: {get_df.shape}")
    return get_df


def query_pub_domains(database_connection):
    # Query Pub Domain Table
    sel_query = """SELECT id, url, crawled_at
        FROM pub_domains
        ORDER BY crawled_at NULLS FIRST
        limit 10000
        """
    df = pd.read_sql(sel_query, database_connection.engine)
    return df


def query_store_apps(stores, database_connection):
    where_str = "store IN (" + (", ").join([str(x) for x in stores]) + ")"
    if stores[0] == 1:
        where_str += " AND installs >= 100000"
    sel_query = f"""SELECT store, id as store_app, store_id, updated_at  
        FROM store_apps
        WHERE {where_str}
        ORDER BY updated_at NULLS FIRST
        limit 1000
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
            values_str = values_str.replace("%", "%%")
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
