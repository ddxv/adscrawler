from sqlalchemy import create_engine
import numpy as np
import pandas as pd
import io
from config import CONFIG, get_logger
from sshtunnel import SSHTunnelForwarder

logger = get_logger(__name__)


def get_db_connection(is_local_db):
    if is_local_db:
        local_port = 5432
    else:
        server = OpenSSHTunnel()
        server.start()
        local_port = str(server.local_bind_port)
    conn = PostgresCon("madrone", "127.0.0.1", local_port)
    return conn


def OpenSSHTunnel():
    with SSHTunnelForwarder(
        (CONFIG["ssh"]["host"], 22),  # Remote server IP and SSH port
        ssh_username=CONFIG["ssh"]["username"],
        ssh_pkey=CONFIG["ssh"]["pkey"],
        ssh_private_key_password=CONFIG["ssh"]["pkey_password"],
        remote_bind_address=("127.0.0.1", 5432),
    ) as server:  # PostgreSQL server IP and sever port on remote machine
        server.start()  # start ssh sever
        logger.info("Connecting via SSH")
        # connect to PostgreSQL
    return server


class PostgresCon:
    """Class for managing the connection to postgres
    Parameters:
    ----------------
        my_db: String, passed on init, string name of db
    """

    engine = None
    db_name = None
    db_pass = None
    db_uri = None
    db_user = None
    db_ip = None
    ssh_pkey_password = None
    ssh_pkey = None
    ssh_username = None

    def __init__(self, my_db, db_ip, db_port):
        self.db_name = my_db
        self.db_ip = db_ip
        self.db_port = db_port
        try:
            self.db_pass = CONFIG["db"][self.db_name]["db_password"]
            self.db_user = CONFIG["db"][self.db_name]["db_user"]
        except Exception as error:
            logger.error(f"Loading db_auth for {self.db_name}, error: {error}")

    def set_engine(self):
        try:
            db_login = f"postgresql://{self.db_user}:{self.db_pass}"
            db_uri = f"{db_login}@{self.db_ip}:{self.db_port}/{self.db_name}"
            logger.info(f"Connecting to PostgreSQL {self.db_name}")
            self.engine = create_engine(db_uri, connect_args={"connect_timeout": 10})
        except Exception as error:
            logger.error(
                f"Failed to connect {self.db_name} @ {self.db_ip}, error: {error}"
            )
            self.db_name = None


def pd_to_psql(df, uri, table_name, if_exists="fail", sep=","):
    """
    Load pandas dataframe into a sql table using native postgres COPY FROM.
    Args:
        df (dataframe): pandas dataframe
        uri (str): postgres psycopg2 sqlalchemy database uri
        table_name (str): table to store data in
        schema_name (str): name of schema in db to write to
        if_exists (str): {‘fail’, ‘replace’, ‘append’}, default ‘fail’.
            See `pandas.to_sql()` for details
        sep (str): separator for temp file, eg ',' or '\t'
    Returns:
        bool: True if loader finished
    """
    if "psycopg2" not in uri:
        raise ValueError(
            "need to use psycopg2 uri, install with `pip install psycopg2-binary`"
        )
    sql_engine = create_engine(uri)
    table_name = table_name.lower()
    sql_cnxn = sql_engine.raw_connection()
    cursor = sql_cnxn.cursor()
    df[:0].to_sql(table_name, sql_engine, if_exists=if_exists, index=False)
    custom_na = r"{CUSTOM_NA}"
    fbuf = io.StringIO()
    df.to_csv(fbuf, index=False, header=False, sep=sep, na_rep=custom_na)
    fbuf.seek(0)
    cursor.copy_from(fbuf, table_name, sep=sep, null=custom_na)
    sql_cnxn.commit()
    cursor.close()


def delete_and_insert(
    df,
    table_name,
    col_del_keys,
    my_db,
    strict_cols=True,
    check_table=True,
    date_based_delete=True,
):
    """
    Parameters
    ----------------
        df: Pandas DataFrame, finalized spend table data, kcids added in DB
        table_name: name of table
        col_del_keys: list of column names to base on deleting
    """
    date_col = "date"
    if table_name in ["nonpostback_revenue", "pb_aggr_revenue"]:
        date_col = "installed_date"
    if "pb_aggr_install" in table_name:
        date_col = "created_date"
    if "pb_aggr_rev" in table_name:
        date_col = "installed_date"
    if date_based_delete:
        assert date_col in df.columns
    if date_col in df.columns:
        df_sd = df[date_col].min()
        df_ed = df[date_col].max()
        where_clause = f"WHERE {date_col} >= '{df_sd}' AND {date_col} <= '{df_ed}'"
    else:
        where_clause = ""
    if "campaign" in df.columns:
        df["campaign"] = df["campaign"].str.replace("\t", "")
    if "network_key" in df.columns:
        df["network_key"] = df["network_key"].str.replace("%", "%%")
        df["network_key"] = np.where(
            df.network_key == "", "emptystring", df.network_key
        )
    for col in col_del_keys:
        col_str = "('" + ("' , '").join(list(df[col].unique())) + "')"
        if "WHERE" in where_clause:
            where_clause += f" AND {col} in {col_str}"
        else:
            where_clause += f" WHERE {col} in {col_str}"
    del_query = f"DELETE FROM {table_name} {where_clause}"
    if check_table:
        exists_query = f"""SELECT EXISTS 
                    (SELECT 1
                    FROM information_schema.tables 
                    WHERE table_name = '{table_name}');
                    """
        r = pd.read_sql(exists_query, my_db.engine)
        if r.exists.values[0]:
            sel_query = f"""select * from {table_name} limit 1;
                    """
            table_meta = pd.read_sql(sel_query, my_db.engine)
            extra_cols = [a for a in df.columns if a not in table_meta.columns]
            miss_cols = [a for a in table_meta.columns if a not in df.columns]
            if len(extra_cols) > 0:
                logger.warning(
                    f"Dropping {len(extra_cols)} columns: eg: {extra_cols[0:3]}"
                )
                df = df.drop(extra_cols, axis=1)
            if len(miss_cols) > 0:
                logger.warning(
                    f"Missing {len(miss_cols)} columns: eg: {miss_cols[0:3]}"
                )
                if not strict_cols:
                    for col in miss_cols:
                        df[col] = np.nan
            df = df[table_meta.columns]
            # Delete
            # logger.info(f"{del_query}")
            del_result = my_db.engine.execute(del_query)
            logger.info(f"DELETED {del_result.rowcount} ROWS")
    else:
        del_result = my_db.engine.execute(del_query)
        logger.info(f"DELETED {del_result.rowcount} ROWS")
    logger.info(f"INSERT {df.shape[0]} ROWS INTO {table_name}")
    connection_uri = (
        f"{my_db.db_user}:{my_db.db_pass}@{my_db.db_ip}:5432/{my_db.db_name}"
    )
    uri = f"postgresql+psycopg2://{connection_uri}"
    pd_to_psql(df, uri, table_name, if_exists="append", sep="\t")
