import datetime

import pandas as pd

from adscrawler.dbcon.connection import get_db_connection
from adscrawler.dbcon.queries import (
    delete_combined_history_by_quarter,
    insert_bulk,
    query_report_combined_domains,
    query_zscores,
)

pgdb = get_db_connection()

start_date = "2026-05-01"
start_date_of_next_period = "2026-05-31"
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
DEFAULT_START_DATE = "2025-01-01"
DEFAULT_START_DATE = "2026-04-01"
today = pd.Timestamp(datetime.date.today())
quarters = pd.date_range(start=DEFAULT_START_DATE, end=today, freq="QS")

for start_date in quarters:
    # end_date is the first day of the NEXT period
    start_of_next_period = start_date + pd.offsets.QuarterEnd() + pd.Timedelta(days=1)

    # Determine the actual start date to pass to the query
    query_start_date = start_date

    # Check if 'today' falls within this quarter's standard range
    buffer_start_date = start_date - pd.Timedelta(weeks=3)
    if start_date <= today < buffer_start_date:
        # Apply the 3-week buffer only for the ongoing quarter
        query_start_date = start_date - pd.Timedelta(weeks=3)
        print("Too early in current quarter detected. Wait for 3-week buffer to start.")
        continue  # Skip the ongoing quarter to avoid incomplete data

    print(
        f"Processing quarter starting on {start_date.date()} to {start_of_next_period.date()}"
    )
    df = query_report_combined_domains(
        pgdb, start_date=start_date, start_of_next_period=start_of_next_period
    )
    df["year"] = start_date.year
    df["quarter"] = start_date.quarter
    delete_combined_history_by_quarter(
        pgdb=pgdb,
        delete_year=start_date.year,
        delete_quarter=start_date.quarter,
    )
    insert_bulk(
        df=df,
        schema="adtech",
        table_name="combined_domain_app_history",
        pgdb=pgdb,
        chunk_size=500000,
    )
