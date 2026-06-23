"""Insert a new company and SDK into the database.

Usage:
    # Insert from YAML config (default)
    python insert_new.py

    # Insert multiple app publishers from CSV
    python insert_new.py --csv-app-publishers
"""

import argparse
import csv

import yaml

from adscrawler.dbcon.connection import get_db_connection
from adscrawler.tools.get_company_logos import process_new_company

pgdb = get_db_connection()


def load_csv(file_path: str) -> list[dict[str, str]]:
    with open(file_path, newline="") as f:
        reader = csv.DictReader(f)
        return list(reader)


def load_yaml_file(file_path: str) -> dict:
    with open(file_path) as file:
        return yaml.safe_load(file)


def insert_company_category_manual(cur, company_id: int, category_id: int) -> None:
    cur.execute(
        """
            INSERT INTO adtech.company_categories_manual (company_id, category_id)
            VALUES (%s, %s)
            ON CONFLICT (company_id) DO NOTHING;
        """,
        (company_id, category_id),
    )


def main():
    parser = argparse.ArgumentParser(
        description="Insert a new company and optionally SDK into the database."
    )
    parser.add_argument(
        "--csv-app-publishers",
        action="store_true",
        help="Load from app_pubs.csv instead of new_company.yml (inserts app publishers, no SDK)",
    )
    args = parser.parse_args()

    if args.csv_app_publishers:
        _main_csv()
    else:
        _main_yaml()


def _main_csv() -> None:
    """Insert multiple publishers from app_pubs.csv (all category_id=7)."""
    rows = load_csv("app_pubs.csv")
    print(f"Loaded {len(rows)} entries from app_pubs.csv")

    with pgdb.get_cursor() as cur:
        for i, row in enumerate(rows, start=1):
            company_name = row["Company"].strip()
            domain = row["Domain"].strip()
            try:
                company_id = insert_company_with_domain(cur, company_name, domain, None)
                insert_company_category_manual(cur, company_id, 7)
                print(
                    f"[{i}/{len(rows)}] Inserted publisher: {company_name} ({domain})"
                )
            except Exception as e:
                print(
                    f"[{i}/{len(rows)}] Error inserting {company_name} ({domain}): {e}"
                )
                continue

    # Process logos and country evidence
    print("\nProcessing company logos and country evidence...")
    for i, row in enumerate(rows, start=1):
        company_name = row["Company"].strip()
        try:
            process_new_company(company_name)
            print(f"[{i}/{len(rows)}] Processed: {company_name}")
        except Exception as e:
            print(f"[{i}/{len(rows)}] Error processing {company_name}: {e}")

    print("\nDone.")


def _main_yaml() -> None:
    """Insert a single company from new_company.yml."""
    data = load_yaml_file("new_company.yml")
    company_name = data["company_name"]
    company_linkedin = data.get("linkedin_url")
    company_github_user = data.get("github_user")
    domain = data["domain"]
    sdk_name = data["sdk_name"]
    sdk_slug = data["sdk_slug"]
    is_open_source = data["is_open_source"]
    has_third_party_tracking = data["has_third_party_tracking"]
    category_id = data["category_id"]
    sdk_package_patterns = data["sdk_package_patterns"]

    try:
        with pgdb.get_cursor() as cur:
            company_id = insert_company_with_domain(
                cur, company_name, domain, company_linkedin, company_github_user
            )

            if category_id == 7:
                # Publisher — skip SDK, just tag the company category
                insert_company_category_manual(cur, company_id, category_id)
                print(f"Inserted publisher category for: {company_name} ({domain})")
            else:
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
        print("Processing company logo and country evidence...")
        process_new_company(company_name)

    except Exception as e:
        print("Error:", e)


def insert_company_with_domain(
    cur,
    company_name: str,
    domain: str,
    company_linkedin: str | None,
    company_github_user: str | None = None,
) -> int:
    # Insert ad_domain if not exists and get its ID
    cur.execute(
        """
            INSERT INTO domains (domain_name) 
            VALUES (%s) 
            ON CONFLICT (domain_name) DO NOTHING
            RETURNING id;
        """,
        (domain,),
    )
    ad_domain_id = cur.fetchone()

    if not ad_domain_id:
        cur.execute("SELECT id FROM domains WHERE domain_name = %s;", (domain,))
        ad_domain_id = cur.fetchone()[0]
    else:
        ad_domain_id = ad_domain_id[0]

    # Insert company if not exists and get its ID
    cur.execute(
        """
            INSERT INTO adtech.companies (name, domain_id, linkedin_url, github_user) 
            VALUES (%s, %s, %s, %s) 
            ON CONFLICT (name) DO NOTHING
            RETURNING id;
        """,
        (company_name, ad_domain_id, company_linkedin, company_github_user),
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
