"""
This script is an example of how to initialize the database with the full database dump and insert the default values.
It also downloads the ddxv/appgoblin-data apps and inserts them into the database.
"""

import requests
import lzma
import pandas as pd
from io import BytesIO
from adscrawler.dbcon.connection import get_db_connection
import os
from adscrawler.config import get_logger
from adscrawler.app_stores.scrape_stores import process_scraped
import numpy as np

logger = get_logger(__name__)

use_tunnel = False
database_connection = get_db_connection(use_ssh_tunnel=use_tunnel)

database_connection.set_engine()

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
url = "https://github.com/ddxv/appgoblin-data/raw/main/data/store_apps.tsv.xz"
response = requests.get(url)
response.raise_for_status()  # Check if the request was successful

with lzma.open(BytesIO(response.content), mode="rt") as file:
    # Step 3: Read the decompressed file into a pandas DataFrame
    df = pd.read_csv(file, sep="\t")


df["store"] = np.where(df["store"] == "google", 1, 2)

app_dict = df[["store", "store_id"]].drop_duplicates().to_dict(orient="records")

logger.info("Insert dddxv/appgoblin-data apps into db")
process_scraped(
    database_connection=database_connection,
    ranked_dicts=app_dict,
    crawl_source="appgoblin_initial_data",
)
