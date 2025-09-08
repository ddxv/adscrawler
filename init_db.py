"""
This script is an example of how to initialize the database with the full database dump and insert the default values.
It also downloads the ddxv/appgoblin-data apps and inserts them into the database.
"""

import lzma
import os
from io import BytesIO

import numpy as np
import pandas as pd
import requests
from sqlalchemy import text

from adscrawler.app_stores.scrape_stores import process_scraped
from adscrawler.config import get_logger
from adscrawler.dbcon.connection import get_db_connection

logger = get_logger(__name__)

use_tunnel = False
database_connection = get_db_connection(use_ssh_tunnel=use_tunnel)


logger.info("Creating database")
os.system("sudo -u postgres createdb madrone")
os.system(
    "sudo -u postgres psql -U postgres -d madrone -f pg-ddl/schema/full_db_dump.sql"
)
logger.info("Creating initial values")
os.system("sudo -u postgres psql -d madrone -f pg-ddl/insert_default_values.sql")
os.system("sudo -u postgres psql -d madrone -f pg-ddl/insert_iso_country.sql")
os.system(
    "sudo -u postgres psql -d madrone -f pg-ddl/insert_collection_and_category.sql"
)


logger.info("Downloading ddxv/appgoblin-data apps")
APPGOBLIN_DATA_URL = (
    "https://github.com/ddxv/appgoblin-data/raw/main/data/store_apps.tsv.xz"
)


def get_appgoblin_data():
    response = requests.get(APPGOBLIN_DATA_URL)
    response.raise_for_status()  # Check if the request was successful
    with lzma.open(BytesIO(response.content), mode="rt") as file:
        # Step 3: Read the decompressed file into a pandas DataFrame
        df = pd.read_csv(file, sep="\t")
    df["store"] = np.where(df["store"] == "google", 1, 2)
    return df[["store", "store_id"]].drop_duplicates().to_dict(orient="records")


app_dict = get_appgoblin_data()

logger.info("Insert dddxv/appgoblin-data apps into db")
process_scraped(
    database_connection=database_connection,
    ranked_dicts=app_dict,
    crawl_source="appgoblin_initial_data",
)

# Set all app updated_at to NULL to so they update at the next crawl
set_app_updated_at = """UPDATE store_apps SET updated_at = NULL;"""

with database_connection.engine.connect() as conn:
    conn.execute(text(set_app_updated_at))

# Refresh all materialized views once to get the db in a good state

with open("pg-ddl/schema/full_db_dump.sql") as f:
    full_db_dump = f.read()

# Find all CREATE MATERIALIZED VIEW statements
create_mv_statements = [
    statement
    for statement in full_db_dump.split(";")
    if "CREATE MATERIALIZED VIEW" in statement
]


for statement in create_mv_statements:
    mv_name = statement.split("CREATE MATERIALIZED VIEW")[1].split(" ")[1]
    with database_connection.engine.connect() as conn:
        trans = conn.begin()
        try:
            conn.execute(text(f"REFRESH MATERIALIZED VIEW {mv_name}"))
            trans.commit()
            print(f"✓ Successfully refreshed: {mv_name}")
        except Exception as e:
            trans.rollback()
            print(f"✗ Failed to refresh {mv_name}: {str(e)}")
