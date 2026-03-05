import pandas as pd

from adscrawler.dbcon.connection import get_db_connection
from adscrawler.dbcon.queries import query_zscores

use_tunnel = True
database_connection = get_db_connection(use_ssh_tunnel=use_tunnel)


start_date = "2026-01-01"
end_date = "2026-01-31"
for week in pd.date_range(start=start_date, end=end_date, freq="W-Mon"):
    df = query_zscores(database_connection, target_week=week)
    break
    df.to_sql(
        "store_app_z_scores_history",
        database_connection.engine,
        if_exists="append",
        index=False,
    )
    print(week)
