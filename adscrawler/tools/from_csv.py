from adscrawler.config import MODULE_DIR, get_logger
import pandas as pd
from adscrawler.queries import upsert_df

logger = get_logger(__name__)


def reinsert_from_csv(filename: str) -> pd.DataFrame:
    filename = f"{MODULE_DIR}/{filename}"
    chunksize = 10000
    i = 0
    store = 2
    platform = 2

    with pd.read_csv(filename, chunksize=chunksize) as reader:
        for chunk in reader:
            logger.info(f"chunk {i}")
            chunk["platform"] = platform
            chunk["store"] = store
            chunk.columns = [x.replace(" ", "_").lower() for x in chunk.columns]
            if store == 1:
                insert_columns = chunk.columns.tolist()
            if store == 2:
                chunk = chunk.rename(
                    columns={
                        "ios_app_id": "app_id",
                        "title": "app_name",
                        "developer_ios_id": "developer_id",
                        "current_version_release_date": "last_updated",
                        "primary_genre": "category",
                        "total_number_of_ratings": "rating_count",
                        "price_usd": "price",
                    }
                )
                insert_columns = [
                    "app_id",
                    "app_name",
                    "developer_id",
                    "rating_count",
                    "last_updated",
                    "category",
                    "platform",
                    "store",
                ]
            upsert_df(
                table_name="app_store_csv_dump",
                insert_columns=insert_columns,
                df=chunk,
                key_columns=["platform", "store", "app_id"],
            )
            i += 1
