import pandas as pd

from adscrawler.dbcon.connection import get_db_connection
from adscrawler.dbcon.queries import query_zscores

pgdb = get_db_connection()

start_date = "2026-02-01"
end_date = "2026-02-28"
for week in pd.date_range(start=start_date, end=end_date, freq="W-MON"):
    df = query_zscores(pgdb, target_week=week)
    df.to_sql(
        "store_app_z_scores_history_2026",
        pgdb.engine,
        if_exists="append",
        index=False,
    )
    print(week)
