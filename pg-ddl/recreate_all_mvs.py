"""
Recreate missing materialized views from individual __matview.sql files.
This script checks which MVs exist in pg-ddl/schema/adtech/*__matview.sql but not in the current database,
then attempts to create them. It stops on the first failure.
"""

import argparse
import re
from pathlib import Path

from sqlalchemy import text
from sqlalchemy.engine import Connection

from adscrawler.config import get_logger
from adscrawler.dbcon.connection import get_db_connection

logger = get_logger(__name__)


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
        file_content_str = re.sub(
            r"\\restrict.*\n?", "", file_content, flags=re.IGNORECASE
        )
        file_content_str = re.sub(
            r"\\unrestrict.*\n?", "", file_content_str, flags=re.IGNORECASE
        )
        mv_name = extract_mv_name_from_file(file_content_str, mv_file)
        if not mv_name:
            logger.warning(f"Could not extract MV name from {mv_file.name}")
            continue
        drop_mv(mv_name)


def has_data(conn: Connection, mv_name: str) -> bool:
    """Check if a materialized view has data. Reuses existing connection."""
    try:
        result = conn.execute(text(f"SELECT 1 FROM {mv_name} LIMIT 1;"))
        return result.first() is not None
    except Exception:
        return False


def check_mvs_in_crontab(mv_files: list[Path]) -> None:
    """Check if the materialized views are in the crontab."""

    # 1. Standardize file-based MV names: 'schema.viewname'
    # Uses split("/")[-1] as per your updated logic
    expected_mvs = {
        f"{f.parent.as_posix().split('/')[-1]}.{f.name.replace('__matview.sql', '')}"
        for f in mv_files
    }

    try:
        with open("pg-ddl/schema/example_contab.txt") as f:
            lines = f.readlines()
    except FileNotFoundError:
        logger.error("Crontab file not found at pg-ddl/schema/example_contab.txt")
        return

    # 2. Identify MVs currently in the crontab file
    # We extract the actual name after 'REFRESH MATERIALIZED VIEW'
    found_in_crontab = set()
    raw_extra_lines = []

    mv_pattern = re.compile(
        r"REFRESH\s+MATERIALIZED\s+VIEW\s+(?:CONCURRENTLY\s+)?([\w\.]+)", re.IGNORECASE
    )

    for line in lines:
        clean_line = line.strip()
        if not clean_line or clean_line.startswith("#"):
            continue

        if "REFRESH MATERIALIZED VIEW" in clean_line:
            match = mv_pattern.search(clean_line)
            if match:
                mv_name = match.group(1)
                found_in_crontab.add(mv_name)
                # If it's in the crontab but not in our file list, it's an extra MV
                if mv_name not in expected_mvs:
                    raw_extra_lines.append(mv_name)
            else:
                # Line has the command but regex failed (edge case)
                raw_extra_lines.append(clean_line)

    # 3. Calculate Missing
    missing_mvs = expected_mvs - found_in_crontab

    # 4. Logging results
    logger.info(
        f"Comparison complete: {len(missing_mvs)} missing, {len(raw_extra_lines)} extra."
    )

    for mv in sorted(missing_mvs):
        logger.warning(f"example_crontab.txt Missing MV: {mv}")

    for line in raw_extra_lines:
        logger.info(f"example_crontab.txt Extra/Unknown Job: {line}")

    return


def create_all_mvs(mv_files: list[Path], stop_on_error: bool = False) -> None:
    """Create all missing materialized views."""
    existing_mvs = get_existing_mvs()
    logger.info(f"Found {len(existing_mvs)} existing MVs")

    if not mv_files:
        logger.error("No matview files found!")
        return

    ordered_mvs = get_correct_order_from_dump(mv_files)

    mvs_missing_in_db = []
    for mv_name in ordered_mvs:
        mv_file = next(
            (f for f in mv_files if mv_name.split(".")[1] + "__matview.sql" in f.name),
            None,
        )

        with open(mv_file) as f:
            file_content = f.read()

        # Remove \restrict line that causes errors
        file_content_str = re.sub(
            r"\\restrict.*\n?", "", file_content, flags=re.IGNORECASE
        )
        file_content_str = re.sub(
            r"\\unrestrict.*\n?", "", file_content_str, flags=re.IGNORECASE
        )
        mv_name = extract_mv_name_from_file(file_content_str, mv_file)
        if not mv_name:
            logger.warning(f"Could not extract MV name from {mv_file.name}")
            continue

        if mv_name not in existing_mvs:
            mvs_missing_in_db.append((mv_name, file_content_str.strip(), mv_file.name))

    logger.info(f"Found {len(mvs_missing_in_db)} missing MVs to create in db")

    with database_connection.engine.connect() as conn:
        for mv_name, create_statement, _file_name in mvs_missing_in_db:
            trans = conn.begin()
            try:
                conn.execute(text(create_statement))
                trans.commit()
                logger.info(f"✓ Successfully created: {mv_name}")
            except Exception as e:
                trans.rollback()
                logger.exception(f"✗ Failed to create {mv_name} with {e}")
                if stop_on_error:
                    logger.error("Stopping further MV creation due to error.")
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


def run_all_mvs(mv_files: list[Path], stop_on_error: bool = False) -> None:
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
        file_content_str = re.sub(
            r"\\restrict.*\n?", "", file_content, flags=re.IGNORECASE
        )
        file_content_str = re.sub(
            r"\\unrestrict.*\n?", "", file_content_str, flags=re.IGNORECASE
        )
        mv_name = extract_mv_name_from_file(file_content_str, mv_file)
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
                if stop_on_error:
                    logger.error("Stopping further MV refresh due to error.")
                    break


def get_mv_index_files() -> list[Path]:
    """Find all __matview_index.sql files in pg-ddl/schema/{public,frontend,adtech}/"""
    schemas = ["public", "frontend", "adtech"]
    all_index_files = []

    for schema in schemas:
        schema_dir = Path(f"pg-ddl/schema/{schema}")
        if not schema_dir.exists():
            logger.warning(f"Directory not found: {schema_dir}")
            continue

        index_files = list(schema_dir.glob("*__matview_index.sql"))
        all_index_files.extend(index_files)

    logger.info(f"Total: {len(all_index_files)} matview index files across all schemas")
    return sorted(all_index_files)


def get_existing_indexes() -> set:
    """Get all existing index names from the database."""
    query = """
        SELECT indexname
        FROM pg_indexes
        WHERE schemaname IN ('public', 'frontend', 'adtech')
    """
    with database_connection.engine.connect() as conn:
        result = conn.execute(text(query))
        return {row[0] for row in result}


def extract_index_statements_from_file(file_content: str) -> list[tuple[str, str]]:
    content = re.sub(r"\\restrict.*\n?", "", file_content, flags=re.IGNORECASE)
    content = re.sub(r"\\unrestrict.*\n?", "", content, flags=re.IGNORECASE)

    results = []
    # Match from CREATE INDEX/UNIQUE INDEX up to the next semicolon
    pattern = re.compile(
        r"(CREATE\s+(?:UNIQUE\s+)?INDEX\s+(?:IF\s+NOT\s+EXISTS\s+)?([a-zA-Z_][a-zA-Z0-9_]*)[^;]+;)",
        re.IGNORECASE | re.DOTALL,
    )
    for match in pattern.finditer(content):
        full_statement = match.group(1).strip()
        index_name = match.group(2)
        if index_name.upper() in ("IF", "ON", "UNIQUE", "INDEX"):
            logger.warning(
                f"Regex captured unexpected keyword '{index_name}' as index name in:\n{full_statement[:120]}"
            )
            continue
        results.append((index_name, full_statement))

    return results


def create_all_mv_indexes(mv_files: list[Path], stop_on_error: bool = False) -> None:
    """
    For each matview SQL file, extract any CREATE INDEX statements and create
    any that don't already exist in the database. Skips indexes that are already present.
    """
    query = """
        SELECT indexname
        FROM pg_indexes
        WHERE schemaname IN ('public', 'frontend', 'adtech')
    """
    with database_connection.engine.connect() as conn:
        result = conn.execute(text(query))
        existing_indexes = {row[0] for row in result}
    logger.info(f"Found {len(existing_indexes)} existing indexes in db")
    all_missing: list[tuple[str, str, str]] = []  # (index_name, statement, source_file)
    for mv_file in mv_files:
        with open(mv_file) as f:
            file_content = f.read()
        for index_name, statement in extract_index_statements_from_file(file_content):
            if index_name in existing_indexes:
                pass
            else:
                all_missing.append((index_name, statement, mv_file.name))
    logger.info(f"Found {len(all_missing)} missing indexes to create")
    with database_connection.engine.connect() as conn:
        for index_name, statement, source_file in all_missing:
            trans = conn.begin()
            try:
                conn.execute(text(statement))
                trans.commit()
                logger.info(f"✓ Created index: {index_name} (from {source_file})")
            except Exception as e:
                trans.rollback()
                logger.exception(
                    f"✗ Failed to create index {index_name} from {source_file}: {e}"
                )
                if stop_on_error:
                    logger.error("Stopping further index creation due to error.")
                    break


def main(
    drop_all: bool,
    create_all: bool,
    run_all: bool,
    stop_on_error: bool,
) -> None:
    mv_files = get_mv_files()

    if drop_all:
        drop_all_mvs(mv_files)

    if create_all:
        create_all_mvs(mv_files, stop_on_error=stop_on_error)
        create_all_mv_indexes(mv_files, stop_on_error=stop_on_error)
        check_mvs_in_crontab(mv_files)

    if run_all:
        run_all_mvs(mv_files, stop_on_error=stop_on_error)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Manage materialized views: drop, create, and refresh"
    )

    parser.add_argument(
        "--drop-all-mvs",
        action="store_true",
        help="Drop all materialized views before creating",
    )
    parser.add_argument(
        "--create-all-mvs",
        action="store_true",
        help="Create missing materialized views",
    )
    parser.add_argument(
        "--run-all-mvs", action="store_true", help="Refresh all materialized views"
    )
    parser.add_argument(
        "--use-tunnel",
        action="store_true",
        help="Use SSH tunnel for database connection",
    )
    parser.add_argument(
        "--config-key",
        type=str,
        required=True,
        help="Database config key (e.g., 'devdb', 'madrone')",
    )
    parser.add_argument(
        "--stop-on-error", action="store_true", help="Stop execution on first error"
    )

    args = parser.parse_args()

    database_connection = get_db_connection(
        use_ssh_tunnel=args.use_tunnel, config_key=args.config_key
    )

    main(
        drop_all=args.drop_all_mvs,
        create_all=args.create_all_mvs,
        run_all=args.run_all_mvs,
        stop_on_error=args.stop_on_error,
    )
