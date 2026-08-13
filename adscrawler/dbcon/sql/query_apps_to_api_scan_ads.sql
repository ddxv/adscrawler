WITH
s3_file_keys AS (
    SELECT * FROM public.s3_file_keys
),
latest_version_codes AS (
    SELECT DISTINCT ON
    (vc.store_app)
        vc.id,
        vc.store_app,
        vc.version_code,
        sfk.myregion,
        sfk.file_key
    FROM
        version_codes AS vc
    INNER JOIN s3_file_keys AS sfk ON vc.id = sfk.version_code_id
    WHERE sfk.myregion = 'loki'
    ORDER BY
        store_app ASC,
        created_at DESC
),
last_scanned AS (
    SELECT DISTINCT ON
    (vc.store_app)
        vasr.version_code_id,
        vc.store_app,
        vasr.run_at,
        vasr.run_result
    FROM
        version_code_api_scan_results AS vasr
    LEFT JOIN version_codes AS vc
        ON vasr.version_code_id = vc.id
    ORDER BY vc.store_app ASC, vasr.run_at DESC
),
last_successful_scanned AS (
    SELECT DISTINCT ON
    (vc.store_app)
        vasr.version_code_id,
        vasr.run_at
    FROM
        version_code_api_scan_results AS vasr
    LEFT JOIN version_codes AS vc
        ON vasr.version_code_id = vc.id
    WHERE
        vasr.run_result = 1
    ORDER BY vc.store_app ASC, vasr.run_at DESC
),
failed_runs AS (
    SELECT
        store_app,
        count(*) AS failed_attempts
    FROM logging.version_code_api_scan_results
    WHERE
        crawl_result != 1
        AND updated_at >= current_date - interval '3 days'
    GROUP BY store_app
),
monthly_ads_scheduled_to_run AS (
    SELECT
        lvc.store_app,
        sa.name,
        sa.store_id,
        lvc.version_code AS version_string,
        lvc.id AS version_code_id,
        agm.total_installs AS installs,
        ls.run_at AS last_run_at,
        fr.failed_attempts,
        ls.run_result AS last_run_result,
        lss.run_at AS last_succesful_run_at
    FROM
        latest_version_codes AS lvc
    LEFT JOIN last_scanned AS ls
        ON
            lvc.store_app = ls.store_app
    LEFT JOIN last_successful_scanned AS lss ON lvc.id = lss.version_code_id
    LEFT JOIN store_apps AS sa
        ON
            lvc.store_app = sa.id
    LEFT JOIN app_global_metrics_latest AS agm
        ON sa.id = agm.store_app
    LEFT JOIN failed_runs AS fr ON sa.id = fr.store_app
    WHERE
        (ls.run_at <= current_date - interval '10 days' OR ls.run_at IS NULL)
        AND sa.store = :store
        AND sa.ad_supported
        AND sa.free
        AND (fr.failed_attempts < 1 OR fr.failed_attempts IS NULL)
        AND sa.id IN (
            SELECT ac.store_app
            FROM creative_records
            LEFT JOIN api_calls AS ac ON creative_records.api_call_id = ac.id
        )
    ORDER BY agm.total_installs DESC NULLS LAST
)
SELECT
    store_app,
    store_id,
    name,
    version_string,
    version_code_id,
    installs,
    last_run_at,
    failed_attempts,
    last_run_result,
    last_succesful_run_at,
    NULL AS user_requested_at,
    'scheduled_ads' AS mysource,
     count(*) OVER () AS total_queue_depth
FROM monthly_ads_scheduled_to_run
LIMIT :mylimit;

