from adscrawler.dbcon.connection import PostgresEngine
from adscrawler.dbcon.queries import get_failed_mitm_logs, log_creative_scan_results
from adscrawler.mitm_ad_parser.mitm_scrape_ads import parse_store_id_mitm_log
from adscrawler.mitm_ad_parser.utils import log_messages_to_df


def retry_failed_mitm_logs(pgdb: PostgresEngine, lookback_days: int = 60) -> None:
    """Retry parsing MITM logs that previously failed.

    Catches all exceptions since failures need to be investigated manually.
    """
    df = get_failed_mitm_logs(pgdb, lookback_days=lookback_days)
    df = df.sort_values(by="inserted_at", ascending=True)
    for _i, row in df.iterrows():
        print(
            f"{_i}/{df.shape[0]} Parsing log with id {row['pub_store_id']} "
            f"for id {row['run_id']}"
        )
        pub_store_id = row["pub_store_id"]
        run_id = row["run_id"]
        try:
            log_messages = parse_store_id_mitm_log(pub_store_id, run_id, pgdb)
        except Exception as e:
            error_msg = f"CRITICAL uncaught error: {e}"
            log_messages = [
                {
                    "run_id": run_id,
                    "pub_store_id": pub_store_id,
                    "error_msg": error_msg,
                }
            ]

        log_msg_df = log_messages_to_df(log_messages, run_id, pub_store_id)
        log_creative_scan_results(log_msg_df, pgdb)
