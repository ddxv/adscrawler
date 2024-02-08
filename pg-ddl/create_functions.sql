CREATE OR REPLACE FUNCTION public.pg_stat_statements(
    showtext boolean,
    OUT userid oid,
    OUT dbid oid,
    OUT toplevel boolean,
    OUT queryid bigint,
    OUT query text,
    OUT plans bigint,
    OUT total_plan_time double precision,
    OUT min_plan_time double precision,
    OUT max_plan_time double precision,
    OUT mean_plan_time double precision,
    OUT stddev_plan_time double precision,
    OUT calls bigint,
    OUT total_exec_time double precision,
    OUT min_exec_time double precision,
    OUT max_exec_time double precision,
    OUT mean_exec_time double precision,
    OUT stddev_exec_time double precision,
    OUT rows bigint,
    OUT shared_blks_hit bigint,
    OUT shared_blks_read bigint,
    OUT shared_blks_dirtied bigint,
    OUT shared_blks_written bigint,
    OUT local_blks_hit bigint,
    OUT local_blks_read bigint,
    OUT local_blks_dirtied bigint,
    OUT local_blks_written bigint,
    OUT temp_blks_read bigint,
    OUT temp_blks_written bigint,
    OUT blk_read_time double precision,
    OUT blk_write_time double precision,
    OUT temp_blk_read_time double precision,
    OUT temp_blk_write_time double precision,
    OUT wal_records bigint,
    OUT wal_fpi bigint,
    OUT wal_bytes numeric,
    OUT jit_functions bigint,
    OUT jit_generation_time double precision,
    OUT jit_inlining_count bigint,
    OUT jit_inlining_time double precision,
    OUT jit_optimization_count bigint,
    OUT jit_optimization_time double precision,
    OUT jit_emission_count bigint,
    OUT jit_emission_time double precision
)
RETURNS SETOF record
LANGUAGE c
PARALLEL SAFE STRICT
AS '$libdir/pg_stat_statements', $function$pg_stat_statements_1_10$function$;

CREATE OR REPLACE FUNCTION public.pg_stat_statements_info(
    OUT dealloc bigint, OUT stats_reset timestamp with time zone
)
RETURNS record
LANGUAGE c
PARALLEL SAFE STRICT
AS '$libdir/pg_stat_statements', $function$pg_stat_statements_info$function$;

CREATE OR REPLACE FUNCTION public.pg_stat_statements_reset(
    userid oid DEFAULT 0, dbid oid DEFAULT 0, queryid bigint DEFAULT 0
)
RETURNS void
LANGUAGE c
PARALLEL SAFE STRICT
AS '$libdir/pg_stat_statements',
$function$pg_stat_statements_reset_1_7$function$;

CREATE OR REPLACE FUNCTION public.process_store_app_audit()
RETURNS trigger
LANGUAGE plpgsql
AS $function$
    BEGIN
--
-- Create a row in store_app_audit to reflect the operation performed on store_apps,
-- making use of the special variable TG_OP to work out the operation.
--
IF (TG_OP = 'DELETE') THEN INSERT INTO logging.store_apps_audit
SELECT
    'D',
    now(),
    USER,
    OLD.id,
    OLD.store,
    OLD.store_id;
ELSIF (TG_OP = 'UPDATE') THEN
INSERT
    INTO
    logging.store_apps_audit
SELECT
    'U',
    now(),
    USER,
    NEW.id,
    NEW.store,
    NEW.store_id,
    NEW.crawl_result;
ELSIF (TG_OP = 'INSERT') THEN
INSERT
    INTO
    logging.store_apps_audit
SELECT
    'I',
    now(),
    USER,
    NEW.id,
    NEW.store,
    NEW.store_id;
END IF;

RETURN NULL;
-- result is ignored since this is an AFTER trigger
END;

$function$;

CREATE OR REPLACE FUNCTION public.snapshot_apps()
RETURNS void
LANGUAGE plpgsql
AS $function$
BEGIN 
WITH alldata AS (
SELECT
    store,
    crawl_result,
    count(*) AS total_rows,
    avg(EXTRACT(DAY FROM now()-updated_at)) AS avg_days,
    max(EXTRACT(DAY FROM now()-updated_at)) AS max_days
FROM
    store_apps
GROUP BY
    store,
    crawl_result),
constb AS (
SELECT
    store,
    crawl_result,
    count(*) AS rows_older_than15
FROM
    store_apps sa
WHERE
    EXTRACT(DAY
FROM
    now()-updated_at) > 15
GROUP BY
    store,
    crawl_result
        )
INSERT
    INTO
    logging.store_apps_snapshot(store,
    crawl_result,
    total_rows,
    avg_days,
    max_days,
    rows_older_than15)
SELECT
    alldata.store,
    alldata.crawl_result,
    alldata.total_rows,
    alldata.avg_days,
    alldata.max_days,
    constb.rows_older_than15
FROM
    alldata
LEFT JOIN constb ON
    alldata.store = constb.store
    AND
    alldata.crawl_result = constb.crawl_result
    ;
END ;

$function$;

CREATE OR REPLACE FUNCTION public.snapshot_pub_domains()
RETURNS void
LANGUAGE plpgsql
AS $function$
BEGIN 
WITH alldata AS (
SELECT
    crawl_result,
    count(*) AS total_rows,
    avg(EXTRACT(DAY FROM now()-updated_at)) AS avg_days,
    COALESCE(max(EXTRACT(DAY FROM now()-updated_at)), 0) AS max_days
FROM
    pub_domains
GROUP BY
    crawl_result
    ),
constb AS (
SELECT
    crawl_result,
    count(*) AS rows_older_than15
FROM
    pub_domains sa
WHERE
    EXTRACT(DAY
FROM
    now()-updated_at) > 15
GROUP BY
    crawl_result
        )
INSERT
    INTO
    logging.snapshot_pub_domains(
    crawl_result,
    total_rows,
    avg_days,
    max_days,
    rows_older_than15
    )
SELECT
    alldata.crawl_result,
    alldata.total_rows,
    alldata.avg_days,
    alldata.max_days,
    constb.rows_older_than15
FROM
    alldata
LEFT JOIN constb ON
    alldata.crawl_result = constb.crawl_result
    ;
END ;

$function$;

CREATE OR REPLACE FUNCTION public.snapshot_store_apps()
RETURNS void
LANGUAGE plpgsql
AS $function$
BEGIN 
WITH alldata AS (
SELECT
    store,
    crawl_result,
    count(*) AS total_rows,
    avg(EXTRACT(DAY FROM now()-updated_at)) AS avg_days,
    COALESCE(max(EXTRACT(DAY FROM now()-updated_at)), 0) AS max_days
FROM
    store_apps
GROUP BY
    store,
    crawl_result),
constb AS (
SELECT
    store,
    crawl_result,
    count(*) AS rows_older_than15
FROM
    store_apps sa
WHERE
    EXTRACT(DAY
FROM
    now()-updated_at) > 15
GROUP BY
    store,
    crawl_result
        )
INSERT
    INTO
    logging.store_apps_snapshot(store,
    crawl_result,
    total_rows,
    avg_days,
    max_days,
    rows_older_than15)
SELECT
    alldata.store,
    alldata.crawl_result,
    alldata.total_rows,
    alldata.avg_days,
    alldata.max_days,
    constb.rows_older_than15
FROM
    alldata
LEFT JOIN constb ON
    alldata.store = constb.store
    AND
    alldata.crawl_result = constb.crawl_result
    ;
END ;

$function$;

CREATE OR REPLACE FUNCTION public.update_crawled_at()
RETURNS trigger
LANGUAGE plpgsql
AS $function$ BEGIN NEW.crawled_at = now();
RETURN NEW;
END;
$function$;

CREATE OR REPLACE FUNCTION public.update_modified_column()
RETURNS trigger
LANGUAGE plpgsql
AS $function$ BEGIN NEW.updated_at = now();

RETURN NEW;

END;

$function$;
