#!/bin/bash
set -euo pipefail

DB=madrone
OUTDIR=/home/ads-db/tmp-pg-ddl

rm -rf "$OUTDIR/"

mkdir -p "$OUTDIR/"

# dump full db
sudo -u postgres pg_dump -U postgres --schema-only --schema=public --schema=logging --schema=adtech --schema=frontend "$DB" > "$OUTDIR/full_db_dump.sql"


# List of schemas to dump
SCHEMAS=("public" "logging" "adtech" "frontend")

# For git diffs and comparison dump all tables/mvs/functions individual
for schema in "${SCHEMAS[@]}"; do
  echo "Dumping schema: $schema"

  # Make sure output directory exists
  mkdir -p "$OUTDIR/$schema"

  # --- Tables ---
  for t in $(sudo -u postgres psql -d "$DB" -Atc "select tablename from pg_tables where schemaname='${schema}'"); do
    echo "  dumping schema table: $schema.$t"
    sudo -u postgres pg_dump -d "$DB" --schema-only --table="${schema}.${t}" > "$OUTDIR/$schema/${t}.sql"
  done

  # --- Materialized Views ---
  for mv in $(sudo -u postgres psql -d "$DB" -Atc "select matviewname from pg_matviews where schemaname='${schema}'"); do
    echo "  dumping schema materialized view: $schema.$mv"
    sudo -u postgres pg_dump -d "$DB" --schema-only --table="${schema}.${mv}" > "$OUTDIR/$schema/${mv}__matview.sql"
  done

  # --- Regular Views (optional, add if you want them too) ---
  for v in $(sudo -u postgres psql -d "$DB" -Atc "select viewname from pg_views where schemaname='${schema}'"); do
    echo "  dumping schema view: $schema.$v"
    sudo -u postgres pg_dump -d "$DB" --schema-only --table="${schema}.${v}" > "$OUTDIR/$schema/${v}__view.sql"
  done

done


