# Schema Sync Script

## Notes for syncing when wanting to update all changes made on remote to git

0. Move to `cd pg-ddl`
1. Remotely run export script. This exports the schema tables/views/functions
   `ssh ads-db 'bash -s' < ./appgoblin_pg_export.sh`

2. SCP to overwrite existing schema dir
   `scp -r ads-db:/home/ads-db/tmp-pg-ddl/* schema/`

3. Run pre-commit across all files. Problem: this is really too slow... 10min plus?
   `pre-commit run --all-files`
   `pre-commit run --files schema/*`

4. Removing MVs? Delete MV files from schema directories.

5. Rerunning MVs? 
   `python pg-ddl/recreate_all_mvs.py --create-all-mvs --config-key madrone`