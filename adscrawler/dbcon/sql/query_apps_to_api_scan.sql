WITH latest_version_codes AS (
    SELECT DISTINCT ON
    (store_app)
        id,
        store_app,
        version_code,
        created_at AS last_downloaded_at,
        crawl_result AS download_result
    FROM
        version_codes
    WHERE
        crawl_result = 1
        -- HACKY FIX only try for apps that have successuflly been downloaded, 
        -- but this table still is all history of version_codes in general
        AND created_at >= '2025-05-01'
    ORDER BY
        store_app ASC,
        created_at DESC,
        string_to_array(version_code, '.')::bigint [] DESC
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
    WHERE
        vc.updated_at >= '2025-05-01'
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
        vc.crawl_result = 1
        AND vc.updated_at >= '2025-05-01'
    ORDER BY vc.store_app ASC, vasr.run_at DESC
),
failed_runs AS (
    SELECT
        store_app,
        count(*) AS failed_attempts
    FROM logging.version_code_api_scan_results
    WHERE
        crawl_result != 1
        AND updated_at >= current_date - interval '10 days'
    GROUP BY store_app
),
all_scheduled_to_run AS (
    SELECT
        lvc.store_app,
        sa.name,
        sa.store_id,
        lvc.version_code AS version_string,
        agm.installs,
        ls.run_at AS last_run_at,
        fr.failed_attempts,
        ls.run_result AS last_run_result,
        lss.run_at AS last_succesful_run_at,
        lvc.last_downloaded_at
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
        (ls.run_at <= current_date - interval '120 days' OR ls.run_at IS NULL)
        AND sa.store = :store
        AND (fr.failed_attempts < 1 OR fr.failed_attempts IS NULL)
    ORDER BY agm.installs DESC
),
monthly_ads_scheduled_to_run AS (
    SELECT
        lvc.store_app,
        sa.name,
        sa.store_id,
        lvc.version_code AS version_string,
        agm.installs,
        ls.run_at AS last_run_at,
        fr.failed_attempts,
        ls.run_result AS last_run_result,
        lss.run_at AS last_succesful_run_at,
        lvc.last_downloaded_at
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
        (ls.run_at <= current_date - interval '29 days' OR ls.run_at IS NULL)
        AND sa.store = :store
        AND sa.ad_supported
        AND sa.free
        AND (fr.failed_attempts < 1 OR fr.failed_attempts IS NULL)
        AND sa.id IN (
            SELECT ac.store_app
            FROM creative_records
            LEFT JOIN api_calls AS ac ON creative_records.api_call_id = ac.id
        )
    ORDER BY agm.installs DESC
),
user_requested_apps_crawl AS (
    SELECT DISTINCT ON (sa.id)
        sa.id AS store_app,
        sa.store_id,
        sa.name,
        lsvc.version_code AS version_string,
        agm.installs,
        ls.run_at AS last_run_at,
        fr.failed_attempts,
        ls.run_result AS last_run_result,
        lss.run_at AS last_succesful_run_at,
        lsvc.last_downloaded_at,
        urs.created_at AS user_requested_at
    FROM
        user_requested_scan AS urs
    LEFT JOIN store_apps AS sa
        ON
            urs.store_id = sa.store_id
    LEFT JOIN app_global_metrics_latest AS agm
        ON sa.id = agm.store_app
    INNER JOIN latest_version_codes AS lsvc
        ON
            sa.id = lsvc.store_app
    LEFT JOIN last_scanned AS ls ON lsvc.id = ls.version_code_id
    LEFT JOIN last_successful_scanned AS lss ON lsvc.id = lss.version_code_id
    LEFT JOIN failed_runs AS fr ON sa.id = fr.store_app
    WHERE
        (
            ls.run_at < urs.created_at
            OR ls.run_at IS NULL
        )
        AND sa.store = :store
        AND (fr.failed_attempts < 1 OR fr.failed_attempts IS NULL
        )
    ORDER BY sa.id ASC, urs.created_at DESC
)
SELECT
    store_app,
    store_id,
    name,
    version_string,
    installs,
    last_run_at,
    failed_attempts,
    last_run_result,
    last_succesful_run_at,
    last_downloaded_at,
    user_requested_at,
    'user' AS mysource
FROM user_requested_apps_crawl
UNION ALL
SELECT
    store_app,
    store_id,
    name,
    version_string,
    installs,
    last_run_at,
    failed_attempts,
    last_run_result,
    last_succesful_run_at,
    last_downloaded_at,
    NULL AS user_requested_at,
    'scheduled' AS mysource
FROM all_scheduled_to_run
UNION ALL
SELECT
    store_app,
    store_id,
    name,
    version_string,
    installs,
    last_run_at,
    failed_attempts,
    last_run_result,
    last_succesful_run_at,
    last_downloaded_at,
    NULL AS user_requested_at,
    'scheduled_ads' AS mysource
FROM monthly_ads_scheduled_to_run AS masr
WHERE
    masr.store_id NOT IN (SELECT urac.store_id FROM user_requested_apps_crawl AS urac);
