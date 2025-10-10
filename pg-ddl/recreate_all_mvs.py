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


def get_existing_mvs():
    """Get list of materialized views that currently exist in the database."""
    query = """
        SELECT schemaname || '.' || matviewname as mv_name
        FROM pg_matviews
        ORDER BY schemaname, matviewname
    """
    with database_connection.engine.connect() as conn:
        result = conn.execute(text(query))
        return {row[0] for row in result}


def get_mv_files():
    """Find all __matview.sql files in pg-ddl/schema/{public,frontend,adtech}/"""
    schemas = ["public", "frontend", "adtech"]
    all_mv_files = []

    for schema in schemas:
        matview_dir = Path(f"schema/{schema}")
        if not matview_dir.exists():
            logger.warning(f"Directory not found: {matview_dir}")
            continue

        mv_files = list(matview_dir.glob("*__matview.sql"))
        logger.info(f"Found {len(mv_files)} matview files in {schema}")
        all_mv_files.extend(mv_files)

    logger.info(f"Total: {len(all_mv_files)} matview files across all schemas")
    return sorted(all_mv_files)


def extract_mv_name_from_file(file_content, file_path):
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


def drop_mv(mv_name):
    """Drop a materialized view by name."""
    drop_statement = f"DROP MATERIALIZED VIEW IF EXISTS {mv_name} CASCADE;"
    with database_connection.engine.connect() as conn:
        trans = conn.begin()
        try:
            conn.execute(text(drop_statement))
            trans.commit()
            logger.info(f"dropped: {mv_name}")
        except Exception as e:
            trans.rollback()
            logger.error(f"Failed to drop {mv_name}: {str(e)}")


def drop_all_mvs(mv_files):
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


def create_all_mvs(mv_files):
    existing_mvs = get_existing_mvs()
    logger.info(f"Found {len(existing_mvs)} existing MVs")

    if not mv_files:
        logger.error("No matview files found!")
        return

    missing_mvs = []
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
            missing_mvs.append((mv_name, file_content.strip(), mv_file.name))

    logger.info(f"Found {len(missing_mvs)} missing MVs to create")

    for mv_name, create_statement, file_name in missing_mvs:
        with database_connection.engine.connect() as conn:
            trans = conn.begin()
            try:
                conn.execute(text(create_statement))
                trans.commit()
                logger.info(f"✓ Successfully created: {mv_name}")
            except Exception as e:
                trans.rollback()
                # if "overview" in mv_name:
                logger.exception(f"✗ Failed to create {mv_name} with {e}")
                break


def main():
    mv_files = get_mv_files()

    # drop_all_mvs(mv_files)
    create_all_mvs(mv_files)


if __name__ == "__main__":
    main()
