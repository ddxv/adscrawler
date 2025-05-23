"""Insert a new company and SDK into the database."""

import yaml

from adscrawler.connection import get_db_connection

use_tunnel = True
database_connection = get_db_connection(use_ssh_tunnel=use_tunnel)


def load_yaml_file(file_path: str) -> dict:
    with open(file_path) as file:
        return yaml.safe_load(file)


def main():
    data = load_yaml_file("new_company.yml")
    company_name = data["company_name"]
    domain = data["domain"]
    sdk_name = data["sdk_name"]
    sdk_slug = data["sdk_slug"]
    is_open_source = data["is_open_source"]
    has_third_party_tracking = data["has_third_party_tracking"]
    category_id = data["category_id"]
    sdk_package_patterns = data["sdk_package_patterns"]
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
            INSERT INTO adtech.companies (name, domain_id) 
            VALUES (%s, %s) 
            ON CONFLICT (name) DO NOTHING
            RETURNING id;
        """,
        (company_name, ad_domain_id),
    )
    company_id = cur.fetchone()

    if not company_id:
        cur.execute("SELECT id FROM adtech.companies WHERE name = %s;", (company_name,))
        company_id = cur.fetchone()[0]
    else:
        company_id = company_id[0]

    # # Insert into mapping table if not exists
    # TODO: Add this back when we want to add multiple additional domains
    # cur.execute(
    #     """
    #         INSERT INTO adtech.company_domain_mapping (company_id, domain_id)
    #         VALUES (%s, %s)
    #         ON CONFLICT (company_id, domain_id) DO NOTHING;
    #     """,
    #     (company_id, ad_domain_id),
    # )

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
        cur.execute("SELECT id FROM adtech.sdks WHERE sdk_name = %s;", (sdk_name,))
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


if __name__ == "__main__":
    main()
