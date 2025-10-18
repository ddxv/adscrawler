import pandas as pd

from adscrawler.dbcon.connection import get_db_connection
from adscrawler.dbcon.queries import query_zscores

use_tunnel = False
database_connection = get_db_connection(use_ssh_tunnel=use_tunnel)


start_date = "2025-08-01"
end_date = "2025-09-30"
for week in pd.date_range(start=start_date, end=end_date, freq="W-Mon"):
    df = query_zscores(database_connection, target_week=week)
    df.to_sql(
        "store_app_z_scores_history",
        database_connection.engine,
        if_exists="append",
        index=False,
    )
    print(week)
