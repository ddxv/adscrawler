import pandas as pd

from adscrawler.dbcon.connection import get_db_connection
from adscrawler.dbcon.queries import query_report_combined_companies, query_zscores

pgdb = get_db_connection()

start_date = "2026-05-01"
start_date_of_next_period = "2026-05-30"
for week in pd.date_range(
    start=start_date, end=start_date_of_next_period, freq="W-MON"
):
    df = query_zscores(pgdb, target_week=week)
    df.to_sql(
        "store_app_z_scores_history_2026",
        pgdb.engine,
        if_exists="append",
        index=False,
    )
    print(week)


# Generate the start of every quarter in 2025
quarters = pd.date_range(start="2026-01-01", end="2026-03-01", freq="QS")

for start_date in quarters:
    # end_date is the first day of the NEXT period
    start_of_next_period = start_date + pd.offsets.QuarterEnd() + pd.Timedelta(days=1)

    df = query_report_combined_companies(
        pgdb, start_of_next_period=start_of_next_period
    )
    df["year"] = start_date.year
    df["quarter"] = start_date.quarter

    df.to_sql(
        "combined_companies_history",
        pgdb.engine,
        if_exists="append",
        index=False,
    )
