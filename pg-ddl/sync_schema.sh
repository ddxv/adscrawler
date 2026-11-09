#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
cd "$script_dir"

if (($#)); then
  echo "Usage: $0" >&2
  exit 2
fi

ssh ads-db 'bash -s' < ./appgoblin_pg_export.sh
scp -r 'ads-db:/home/ads-db/tmp-pg-ddl/*' schema/
