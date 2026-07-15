from typing import Any

import pandas as pd
import tldextract

LOG_MSG_COLUMNS = [
    "url",
    "tld_url",
    "path",
    "content_type",
    "run_id",
    "pub_store_id",
    "file_extension",
    "creative_size",
    "error_msg",
]


def get_tld(url: str) -> str | None:
    """Extracts the top-level domain from a URL."""
    tld = tldextract.extract(url)
    if not tld.suffix:
        return None
    tld_url: str = tld.domain + "." + tld.suffix
    if tld_url == ".":
        return None
    return tld_url


def log_messages_to_df(
    log_messages: list[dict[str, Any] | pd.Series],
    run_id: int,
    pub_store_id: str,
) -> pd.DataFrame:
    """Convert a list of error message dicts/series into a normalized DataFrame.

    Handles mixed types (dicts and pd.Series) and ensures the output
    contains only the columns expected by log_creative_scan_results.
    """
    if len(log_messages) > 0:
        d = [x for x in log_messages if isinstance(x, dict)]
        s = [x for x in log_messages if isinstance(x, pd.Series)]
        log_msg_df = pd.concat([pd.DataFrame(d), pd.DataFrame(s)], ignore_index=True)
    else:
        log_msg_df = pd.DataFrame(
            {
                "run_id": [run_id],
                "pub_store_id": [pub_store_id],
                "error_msg": ["No logs!"],
            }
        )
    mycols = [x for x in log_msg_df.columns if x in LOG_MSG_COLUMNS]
    return log_msg_df[mycols]
