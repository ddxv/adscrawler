"""
Recreate missing materialized views from individual __matview.sql files.
This script checks which MVs exist in pg-ddl/schema/adtech/*__matview.sql but not in the current database,
then attempts to create them. It stops on the first failure.
"""

import re
from pathlib import Path

from sqlalchemy import text

from adscrawler.config import get_logger
from adscrawler.dbcon.connection import get_db_connection

logger = get_logger(__name__)

use_tunnel = True
database_connection = get_db_connection(use_ssh_tunnel=use_tunnel)


def get_existing_mvs() -> set:
    """Get list of materialized views that currently exist in the database."""
    query = """
        SELECT schemaname || '.' || matviewname as mv_name
        FROM pg_matviews
        ORDER BY schemaname, matviewname
    """
    with database_connection.engine.connect() as conn:
        result = conn.execute(text(query))
        return {row[0] for row in result}


def get_mv_files() -> list[Path]:
    """Find all __matview.sql files in pg-ddl/schema/{public,frontend,adtech}/"""
    schemas = ["public", "frontend", "adtech"]
    all_mv_files = []

    for schema in schemas:
        matview_dir = Path(f"pg-ddl/schema/{schema}")
        if not matview_dir.exists():
            logger.warning(f"Directory not found: {matview_dir}")
            continue

        mv_files = list(matview_dir.glob("*__matview.sql"))
        all_mv_files.extend(mv_files)

    logger.info(f"Total: {len(all_mv_files)} matview files across all schemas")
    return sorted(all_mv_files)


def extract_mv_name_from_file(file_content: str, file_path: str) -> str | None:
    """Extract the materialized view name from CREATE MATERIALIZED VIEW statement."""
    name_match = re.search(
        r"CREATE MATERIALIZED VIEW\s+(?:IF NOT EXISTS\s+)?([a-zA-Z_][a-zA-Z0-9_.]*)",
        file_content,
        re.IGNORECASE,
    )
    if name_match:
        view_name = name_match.group(1)
        # Add schema if not present
        if "." not in view_name:
            # Infer schema from file path
            path_parts = Path(file_path).parts
            if "adtech" in path_parts:
                view_name = f"adtech.{view_name}"
            elif "frontend" in path_parts:
                view_name = f"frontend.{view_name}"
            else:
                view_name = f"public.{view_name}"
        return view_name
    return None


def drop_mv(mv_name: str) -> None:
    """Drop a materialized view by name."""
    drop_statement = f"DROP MATERIALIZED VIEW IF EXISTS {mv_name} CASCADE;"
    with database_connection.engine.connect() as conn:
        try:
            conn.execute(text(drop_statement))
            conn.commit()
            logger.info(f"dropped: {mv_name}")
        except Exception as e:
            conn.rollback()
            logger.error(f"Failed to drop {mv_name}: {str(e)}")


def drop_all_mvs(mv_files: list[Path]) -> None:
    dont_drop = ["audit_dates"]
    for mv_file in mv_files:
        if mv_file in dont_drop:
            continue
        with open(mv_file) as f:
            file_content = f.read()
        # Remove \restrict line that causes errors
        file_content = re.sub(r"\\restrict.*\n?", "", file_content, flags=re.IGNORECASE)
        file_content = re.sub(
            r"\\unrestrict.*\n?", "", file_content, flags=re.IGNORECASE
        )
        mv_name = extract_mv_name_from_file(file_content, mv_file)
        if not mv_name:
            logger.warning(f"Could not extract MV name from {mv_file.name}")
            continue
        drop_mv(mv_name)


def has_data(conn, mv_name):
    """Check if a materialized view has data. Reuses existing connection."""
    try:
        result = conn.execute(text(f"SELECT 1 FROM {mv_name} LIMIT 1;"))
        return result.first() is not None
    except Exception:
        return False


def create_all_mvs(mv_files: list[Path]) -> None:
    """Create all missing materialized views."""
    existing_mvs = get_existing_mvs()
    logger.info(f"Found {len(existing_mvs)} existing MVs")

    if not mv_files:
        logger.error("No matview files found!")
        return

    mvs_missing_in_db = []
    for mv_file in mv_files:
        with open(mv_file) as f:
            file_content = f.read()

        # Remove \restrict line that causes errors
        file_content = re.sub(r"\\restrict.*\n?", "", file_content, flags=re.IGNORECASE)
        file_content = re.sub(
            r"\\unrestrict.*\n?", "", file_content, flags=re.IGNORECASE
        )
        mv_name = extract_mv_name_from_file(file_content, mv_file)
        if not mv_name:
            logger.warning(f"Could not extract MV name from {mv_file.name}")
            continue

        if mv_name not in existing_mvs:
            mvs_missing_in_db.append((mv_name, file_content.strip(), mv_file.name))

    logger.info(f"Found {len(mvs_missing_in_db)} missing MVs to create in db")

    with database_connection.engine.connect() as conn:
        for mv_name, create_statement, file_name in mvs_missing_in_db:
            trans = conn.begin()
            try:
                conn.execute(text(create_statement))
                trans.commit()
                logger.info(f"✓ Successfully created: {mv_name}")
            except Exception as e:
                trans.rollback()
                logger.exception(f"✗ Failed to create {mv_name} with {e}")
                break


def get_correct_order_from_dump(mv_files: list[Path]) -> list[str]:
    with open("pg-ddl/schema/full_db_dump.sql") as f:
        full_db_dump = f.read()

    # Find all CREATE MATERIALIZED VIEW statements
    create_mv_statements = [
        statement
        for statement in full_db_dump.split(";")
        if "CREATE MATERIALIZED VIEW" in statement
    ]

    ordered_mv_names = []
    for statement in create_mv_statements:
        mv_name = extract_mv_name_from_file(statement, "full_db_dump.sql")
        if mv_name:
            ordered_mv_names.append(mv_name)

    # Filter to only those in mv_files
    mv_file_names = set()
    for mv_file in mv_files:
        with open(mv_file) as f:
            file_content = f.read()
        mv_name = extract_mv_name_from_file(file_content, mv_file)
        if mv_name:
            mv_file_names.add(mv_name)

    ordered_existing_mvs = [mv for mv in ordered_mv_names if mv in mv_file_names]
    return ordered_existing_mvs


def run_all_mvs(mv_files: list[Path]) -> None:
    """Refresh all materialized views in correct dependency order."""
    ordered_mvs = get_correct_order_from_dump(mv_files)

    for mv_name in ordered_mvs:
        mv_file = next(
            (f for f in mv_files if mv_name.split(".")[1] + "__matview.sql" in f.name),
            None,
        )
        if not mv_file:
            logger.warning(f"Could not find SQL file for {mv_name}")
            continue
        with open(mv_file) as f:
            file_content = f.read()
        # Remove \restrict line that causes errors
        file_content = re.sub(r"\\restrict.*\n?", "", file_content, flags=re.IGNORECASE)
        file_content = re.sub(
            r"\\unrestrict.*\n?", "", file_content, flags=re.IGNORECASE
        )
        mv_name = extract_mv_name_from_file(file_content, mv_file)
        if not mv_name:
            logger.warning(f"Could not extract MV name from {mv_file.name}")
            continue

        # Check if already has data (use fresh connection for check)
        with database_connection.engine.connect() as conn:
            if has_data(conn, mv_name):
                logger.info(f"{mv_name} already has data")
                continue

        # Skip adstxt views
        if "adstxt" in mv_name:
            logger.info(f"Skipping adstxt view: {mv_name}")
            continue

        # Refresh the materialized view (use fresh connection for each refresh)
        with database_connection.engine.connect() as conn:
            trans = conn.begin()
            try:
                query = f"REFRESH MATERIALIZED VIEW {mv_name};"
                logger.info(f"{query} start")
                conn.execute(text(query))
                trans.commit()
                logger.info(f"{query} done")
            except Exception as e:
                trans.rollback()
                logger.exception(f"Failed to refresh {mv_name} with {e}")


def main() -> None:
    mv_files = get_mv_files()

    # drop_all_mvs(mv_files)
    # create_all_mvs(mv_files)
    run_all_mvs(mv_files)


if __name__ == "__main__":
    main()
