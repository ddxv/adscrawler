WITH latest_version_codes AS (
    SELECT DISTINCT ON
    (vc_1.store_app)
        vc_1.id,
        vc_1.store_app,
        vc_1.version_code,
        vc_1.updated_at,
        vc_1.created_at,
        vc_1.crawl_result,
        sa.store,
        sa.store_id
    FROM
        version_codes AS vc_1
    LEFT JOIN store_apps AS sa ON vc_1.store_app = sa.id
    WHERE
        vc_1.version_code != '-1'
        AND sa.store = :store
        AND vc_1.created_at < current_date - INTERVAL '3 days'
    ORDER BY
        vc_1.store_app ASC,
        vc_1.created_at DESC
),
last_scan AS (
    SELECT DISTINCT ON
    (lsscr.version_code_id)
        lsscr.version_code_id AS version_code,
        lsscr.scanned_at
    FROM
        version_code_sdk_scan_results AS lsscr
    INNER JOIN latest_version_codes AS vc
        ON
            lsscr.version_code_id = vc.id
    ORDER BY
        lsscr.version_code_id ASC,
        lsscr.scanned_at DESC
),
results AS (
    SELECT
        lvc.*,
        ls.scanned_at
    FROM
        latest_version_codes AS lvc
    LEFT JOIN last_scan AS ls
        ON lvc.id = ls.version_code
    WHERE
        ls.scanned_at < current_date - INTERVAL '3 days'
        OR ls.scanned_at IS NULL
    ORDER BY
        lvc.store_app
)
SELECT
    lvc.store_app,
    lvc.store,
    lvc.store_id,
    lvc.id AS latest_version_code_db_id,
    lvc.version_code AS version_code_str
FROM results AS lvc
LEFT JOIN adtech.app_sdk_strings AS s ON lvc.store_app = s.store_app
WHERE s.store_app IS NULL;
