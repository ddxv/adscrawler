from adscrawler.connection import get_db_connection

use_tunnel = True
database_connection = get_db_connection(use_ssh_tunnel=use_tunnel)


""" 
adtech.categories:
4	Development Tools	development-tools
2	Analytics: Attribution	ad-attribution
1	Ad Networks	ad-networks
5	Business Tools	business-tools
3	Analytics: Product	product-analytics
"""

domain = "verint.com"
company_name = "Verint"
sdk_name = "Verint SDK"
sdk_slug = "verint-sdk"
is_open_source = False
has_third_party_tracking = True
category_id = 5

sdk_package_patterns = [
    "com.verint.xm.sdk.predictive.tracker",
    "com.verint",
    "com.foresee",
    "ForeSeeCxMeasure.framework",
    "react_native_foresee_sdk.framework",
    "ForeSee.framework",
    "ForeSeeLegacy.framework",
    "ForeSeeUtilities.framework",
]


def main():
    try:
        with database_connection.get_cursor() as cur:
            company_id = insert_company_with_domain(cur, company_name, domain)
            sdk_id = insert_sdk(
                cur,
                sdk_name,
                sdk_slug,
                company_id,
                is_open_source,
                has_third_party_tracking,
                category_id,
            )
            insert_sdk_with_package_patterns(cur, sdk_id, sdk_package_patterns)
        print(f"Successfully inserted: {company_name} ({domain})")

    except Exception as e:
        print("Error:", e)


def insert_company_with_domain(cur, company_name: str, domain: str) -> int:
    # Insert ad_domain if not exists and get its ID
    cur.execute(
        """
            INSERT INTO ad_domains (domain) 
            VALUES (%s) 
            ON CONFLICT (domain) DO NOTHING
            RETURNING id;
        """,
        (domain,),
    )
    ad_domain_id = cur.fetchone()

    if not ad_domain_id:
        cur.execute("SELECT id FROM ad_domains WHERE domain = %s;", (domain,))
        ad_domain_id = cur.fetchone()[0]
    else:
        ad_domain_id = ad_domain_id[0]

    # Insert company if not exists and get its ID
    cur.execute(
        """
            INSERT INTO adtech.companies (name) 
            VALUES (%s) 
            ON CONFLICT (name) DO NOTHING
            RETURNING id;
        """,
        (company_name,),
    )
    company_id = cur.fetchone()

    if not company_id:
        cur.execute("SELECT id FROM adtech.companies WHERE name = %s;", (company_name,))
        company_id = cur.fetchone()[0]
    else:
        company_id = company_id[0]

    # Insert into mapping table if not exists
    cur.execute(
        """
            INSERT INTO adtech.company_domain_mapping (company_id, domain_id)
            VALUES (%s, %s)
            ON CONFLICT (company_id, domain_id) DO NOTHING;
        """,
        (company_id, ad_domain_id),
    )

    return company_id


def insert_sdk(
    cur,
    sdk_name: str,
    sdk_slug: str,
    company_id: int,
    is_open_source: bool,
    has_third_party_tracking: bool,
    category_id: int,
) -> int:
    cur.execute(
        """
            INSERT INTO adtech.sdks (sdk_name, sdk_slug, company_id, is_open_source, has_third_party_tracking)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (company_id, sdk_name) DO NOTHING
            RETURNING id;
        """,
        (sdk_name, sdk_slug, company_id, is_open_source, has_third_party_tracking),
    )
    sdk_id = cur.fetchone()

    if not sdk_id:
        cur.execute("SELECT id FROM adtech.sdks WHERE name = %s;", (sdk_name,))
        sdk_id = cur.fetchone()[0]
    else:
        sdk_id = sdk_id[0]

    cur.execute(
        """
            INSERT INTO adtech.sdk_categories (sdk_id, category_id)
            VALUES (%s, %s)
            ON CONFLICT (sdk_id, category_id) DO NOTHING;
        """,
        (sdk_id, category_id),
    )

    return sdk_id


def insert_sdk_with_package_patterns(
    cur, sdk_id: int, sdk_package_patterns: list[str]
) -> None:
    for package_pattern in sdk_package_patterns:
        cur.execute(
            """
                INSERT INTO adtech.sdk_packages (sdk_id, package_pattern)
                VALUES (%s, %s)
                ON CONFLICT (package_pattern) DO NOTHING;
            """,
            (sdk_id, package_pattern),
        )


main()
