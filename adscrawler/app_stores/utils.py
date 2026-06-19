"""Shared utilities for app stores."""

import re
import tldextract

import pandas as pd

from adscrawler.config import get_logger
from adscrawler.dbcon.connection import PostgresEngine
from adscrawler.dbcon.queries import query_countries, query_store_id_map, upsert_df

logger = get_logger(__name__, "scrape_stores")

# ---------------------------------------------------------------------------
# Country guessing helpers (shared with tools/get_company_logos.py)
# ---------------------------------------------------------------------------


def build_name_to_alpha2(countries_df: pd.DataFrame) -> dict[str, str]:
    """Build a lowercase-name -> alpha2 lookup from all language columns."""
    lang_cols = [
        c for c in countries_df.columns
        if c.startswith("lang") and c != "langen"
    ] + ["langen"]
    name_map: dict[str, str] = {}
    for _, row in countries_df.iterrows():
        alpha2 = str(row["alpha2"]).strip().upper()
        if not alpha2:
            continue
        name_map[alpha2] = alpha2
        alpha3 = str(row.get("alpha3", "")).strip().upper()
        if alpha3:
            name_map[alpha3] = alpha2
        for col in lang_cols:
            val = row.get(col)
            if val and isinstance(val, str) and val.strip():
                key = val.strip().lower()
                name_map[key] = alpha2
    return name_map


def guess_country_local(
    address_string: str,
    name_to_alpha2: dict[str, str] | None = None,
) -> str | None:
    """Guess the ISO alpha-2 country code from an address string.

    Uses a pre-built name->alpha2 lookup (from build_name_to_alpha2)
    for ISO lookups and falls back to common aliases.
    """
    if not address_string or not address_string.strip():
        return None

    normalized_address = address_string.lower()

    # 1. Handle common edge-case aliases first
    aliases = {
        "korea, south": "KR",
        "south korea": "KR",
        "republic of korea": "KR",
        "usa": "US",
        "united states of america": "US",
        "uk": "GB",
        "united kingdom": "GB",
    }
    for alias, alpha2 in aliases.items():
        if alias in normalized_address:
            return alpha2

    # 2. If we have a name map, check full names in all languages
    if name_to_alpha2 is not None:
        for name, alpha2 in sorted(
            name_to_alpha2.items(), key=lambda x: -len(x[0])
        ):
            if name in normalized_address:
                return alpha2

    # 3. Check tokens from right to left (countries are usually at the end)
    tokens = re.findall(r"\b\w+\b", address_string.upper())
    for token in reversed(tokens):
        if name_to_alpha2 is not None and token in name_to_alpha2:
            return name_to_alpha2[token]
        if len(token) == 2 and token.isalpha() and name_to_alpha2 is None:
            return token

    return None


def build_country_map(pgdb: PostgresEngine) -> tuple[dict[str, int], dict[str, str]]:
    """Build (alpha2 -> countries.id, name -> alpha2) mappings."""
    countries_df = query_countries(pgdb=pgdb)
    id_map: dict[str, int] = {}
    for _, row in countries_df.iterrows():
        alpha2 = str(row["alpha2"]).strip().upper()
        if alpha2:
            id_map[alpha2] = int(row["id"])
    name_map = build_name_to_alpha2(countries_df)
    return id_map, name_map


def resolve_country_id(
    address_string: str,
    country_id_map: dict[str, int],
    name_to_alpha2: dict[str, str],
) -> int | None:
    """Guess alpha-2 from an address, then map to countries.id."""
    alpha2 = guess_country_local(address_string, name_to_alpha2)
    if alpha2 and alpha2 in country_id_map:
        return country_id_map[alpha2]
    return None


# ---------------------------------------------------------------------------
# Existing utilities below
# ---------------------------------------------------------------------------


def truncate_utf8_bytes(s: str, max_bytes: int = 2400) -> str:
    if s is None:
        return ""
    encoded = s.encode("utf-8")
    if len(encoded) <= max_bytes:
        return s
    # Truncate bytes, then decode safely
    truncated = encoded[:max_bytes]
    while True:
        try:
            return truncated.decode("utf-8")
        except UnicodeDecodeError:
            truncated = truncated[:-1]


def check_and_insert_new_apps(
    dicts: list[dict],
    pgdb: PostgresEngine,
    crawl_source: str,
    store: int,
) -> None:
    df = pd.DataFrame(dicts)
    if store in [1, 2]:
        df["store"] = store
    else:
        raise ValueError(f"Invalid store: {store}")
    found_bad_ids = df.loc[df["store"] == 2, "store_id"].str.match(r"^[0-9].*\.").any()
    if found_bad_ids:
        logger.error(f"Scrape {store=} {crawl_source=} found bad store_ids")
        raise ValueError("Found bad store_ids")
    all_scraped_ids = df["store_id"].unique().tolist()
    existing_ids_map = query_store_id_map(
        pgdb,
        store_ids=all_scraped_ids,
    )
    existing_store_ids = existing_ids_map["store_id"].tolist()
    new_apps_df = df[~(df["store_id"].isin(existing_store_ids))][
        ["store", "store_id"]
    ].drop_duplicates()
    if new_apps_df.empty:
        logger.info(f"Scrape {store=} {crawl_source=} no new apps")
        return
    logger.info(
        f"Scrape {store=} {crawl_source=} insert new apps to db {new_apps_df.shape[0]:,}",
    )
    insert_columns = ["store", "store_id"]
    inserted_apps: pd.DataFrame = upsert_df(
        table_name="store_apps",
        insert_columns=insert_columns,
        df=new_apps_df,
        key_columns=insert_columns,
        pgdb=pgdb,
        return_rows=True,
    )
    if inserted_apps is not None and not inserted_apps.empty:
        inserted_apps["crawl_source"] = crawl_source
        inserted_apps = inserted_apps.rename(columns={"id": "store_app"})
        insert_columns = ["store", "store_app"]
        upsert_df(
            table_name="store_app_sources",
            insert_columns=insert_columns + ["crawl_source"],
            df=inserted_apps,
            key_columns=insert_columns,
            pgdb=pgdb,
            schema="logging",
        )
    return None


def get_parquet_paths_by_prefix(bucket: str, prefix: str) -> list[str]:
    from adscrawler.packages.storage import get_s3_client

    s3 = get_s3_client()
    continuation_token = None
    all_parquet_paths = []
    while True:
        params = {
            "Bucket": bucket,
            "Prefix": prefix,
            "MaxKeys": 1000,
        }
        if continuation_token:
            params["ContinuationToken"] = continuation_token
        response = s3.list_objects_v2(**params)
        # Extract parquet paths from this page
        if "Contents" in response:
            parquet_paths = [
                f"s3://{bucket}/{obj['Key']}"
                for obj in response["Contents"]
                if obj["Key"].endswith(".parquet")
            ]
            all_parquet_paths += parquet_paths
        if "NextContinuationToken" in response:
            continuation_token = response["NextContinuationToken"]
        else:
            break
    return all_parquet_paths

def extract_root_domain(url: str) -> str | None:
    """Extracts the top-level domain from a URL."""
    tld = tldextract.extract(url)
    if not tld.suffix:
        return None
    tld_url: str = tld.domain + "." + tld.suffix
    if tld_url == ".":
        return None
    return tld_url

def extract_domains_with_sub(x: str | None) -> str | None:
    if x is None or pd.isna(x):
        return None

    ext = tldextract.extract(x)
    use_top_domain = any(
        [ext.subdomain == "m", "www" in ext.subdomain.split("."), ext.subdomain == ""],
    )
    if use_top_domain:
        url = ".".join([ext.domain, ext.suffix])
    else:
        url = ".".join([ext.subdomain, ext.domain, ext.suffix])
    url = url.lower()
    return url

