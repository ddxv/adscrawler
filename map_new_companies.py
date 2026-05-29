import json
from itertools import zip_longest
from pathlib import Path

import pandas as pd

from adscrawler.dbcon.connection import get_db_connection

pgdb = get_db_connection()


def query_company_tag_stats(pgdb) -> pd.DataFrame:
    query = """
   SELECT company_domain, company_name, sum(app_count) FROM frontend.companies_category_tag_stats
       WHERE tag_source = 'api_call' OR tag_source = 'publisher'
       GROUP by company_domain, company_name;
    """
    return pd.read_sql(query, pgdb.engine)


def query_company_domain_mapping(pgdb) -> pd.DataFrame:
    query = """
    SELECT cdm.company_id, d.domain_name, c.parent_company_id 
    FROM adtech.company_domain_mapping cdm
    LEFT JOIN domains d ON
        d.id = cdm.domain_id
    LEFT JOIN adtech.companies c ON
        cdm.company_id = c.id
    """
    return pd.read_sql(query, pgdb.engine)


def query_sdk_package_patterns(pgdb) -> pd.DataFrame:
    query = """
    SELECT
        sd.id AS sdk_id,
        sd.company_id,
        sp.package_pattern
    FROM
        adtech.sdks sd
    LEFT JOIN adtech.sdk_packages sp ON
        sd.id = sp.sdk_id 
    """
    return pd.read_sql(query, pgdb.engine)


domains = Path("domains.json")
with domains.open("r", encoding="utf-8") as infile:
    domains_payload = json.load(infile)

domains_records = []
for entity_name, entity_data in domains_payload.get("entities", {}).items():
    for property_value, resource_value in zip_longest(
        entity_data.get("properties", []),
        entity_data.get("resources", []),
    ):
        domains_records.append(
            {
                "entity": entity_name,
                "property": property_value,
                "resource": resource_value,
            }
        )

domains_df = pd.DataFrame(domains_records)

all_domains_to_check = set(
    domains_df["property"].unique().tolist() + domains_df["resource"].unique().tolist()
)


company_domain_mapping_df = query_company_domain_mapping(pgdb)
sdk_package_patterns_df = query_sdk_package_patterns(pgdb)

domains_to_check = company_domain_mapping_df[
    ~company_domain_mapping_df["domain_name"].isin(all_domains_to_check)
]
