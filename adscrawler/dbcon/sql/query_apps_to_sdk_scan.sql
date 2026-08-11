WITH s3_file_keys AS (
    SELECT * FROM public.s3_file_keys
),
all_version_codes AS (
    SELECT
        vc.id,
        vc.store_app,
        vc.version_code,
        sfk.myregion,
        sfk.file_key,
        vc.created_at AS downloaded_at
    FROM
        version_codes AS vc
    INNER JOIN s3_file_keys AS sfk ON
        vc.id = sfk.version_code_id
),
latest_version_codes AS (
    SELECT DISTINCT ON
    (store_app)
        id,
        store_app,
        version_code,
        myregion,
        file_key,
        downloaded_at
    FROM
        all_version_codes
    ORDER BY
        store_app ASC,
        downloaded_at DESC
),
vc_last_scan AS (
    SELECT DISTINCT ON
    (lsscr.version_code_id)
        lsscr.version_code_id,
        vc.store_app,
        lsscr.version_code_id AS version_code,
        lsscr.scanned_at,
        lsscr.scan_result
    FROM
        version_code_sdk_scan_results AS lsscr
    INNER JOIN all_version_codes AS vc
        ON
            lsscr.version_code_id = vc.id
    ORDER BY
        lsscr.version_code_id ASC,
        lsscr.scanned_at DESC
),
vc_last_scan_succeed AS (
    SELECT DISTINCT ON
    (vcss.version_code_id)
        vcss.version_code_id,
        vc.store_app,
        vc.version_code,
        vcss.scanned_at,
        vcss.scan_result
    FROM
        version_code_sdk_scan_results AS vcss
    INNER JOIN
        all_version_codes AS vc
        ON
            vcss.version_code_id = vc.id
    WHERE
        vcss.scan_result = 1
    ORDER BY
        vcss.version_code_id ASC,
        vcss.scanned_at DESC
),
scheduled_vcs_crawl AS (
    SELECT
        vc.id AS version_code_db_id,
        vc.store_app,
        vc.version_code AS version_code_str,
        sa.store_id,
        sa.name,
        sa.installs,
        sa.rating_count,
        'regular' AS mysource,
        ls.scan_result AS last_analyzed_result,
        ls.scanned_at AS last_scanned_at,
        lsvc.scanned_at AS last_scuccess_scanned_at
    FROM
        all_version_codes AS vc
    LEFT JOIN frontend.store_apps_overview AS sa ON vc.store_app = sa.id
    LEFT JOIN vc_last_scan AS ls
        ON
            vc.id = ls.version_code_id
    LEFT JOIN vc_last_scan_succeed AS lsvc
        ON vc.id = lsvc.version_code_id
    WHERE
        sa.store = :store
        AND
        (
            ls.scanned_at IS NULL
            OR
            (
                (
                    lsvc.scan_result = 1
                    AND lsvc.scanned_at < current_date - INTERVAL '180 days'
                    AND ls.scanned_at < current_date - INTERVAL '5 days'
                )
                OR
                (
                    ls.scan_result IN (
                        2, 3, 4
                    )
                    AND (
                        (
                            lsvc.scanned_at < current_date - INTERVAL '90 days'
                            OR lsvc.scanned_at IS NULL
                        )
                        AND ls.scanned_at < current_date - INTERVAL '2 days'
                    )
                )
            )
        )
    ORDER BY
        greatest(
            coalesce(sa.installs, 0),
            coalesce(sa.rating_count::BIGINT, 0) * 50
        )
        DESC NULLS LAST
),
user_requested_apps_crawl AS (
    SELECT DISTINCT ON
    (sa.id)
        sa.id AS store_app,
        urs.created_at AS user_requested_at,
        sa.store_id,
        sa.name,
        lvc.version_code AS version_code_str,
        agm.total_installs AS installs,
        agm.total_ratings AS rating_count,
        'user' AS mysource,
        ls.scan_result AS last_analyzed_result,
        ls.scanned_at AS last_scanned_at,
        lsvc.scanned_at AS last_scuccess_scanned_at,
        lvc.id AS version_code_db_id,
        lvc.downloaded_at
    FROM
        agadmin.user_requested_scan AS urs
    LEFT JOIN store_apps AS sa
        ON
            urs.store_id = sa.store_id
    LEFT JOIN app_global_metrics_latest AS agm
        ON
            sa.id = agm.store_app
    INNER JOIN latest_version_codes AS lvc
        ON sa.id = lvc.store_app
    LEFT JOIN vc_last_scan AS ls
        ON
            lvc.id = ls.version_code_id
    LEFT JOIN vc_last_scan_succeed AS lsvc
        ON
            lvc.id = lsvc.version_code_id
    WHERE
        (
            --  will retrigger for old downloads
            lsvc.scanned_at < urs.created_at
            OR lsvc.scanned_at IS NULL
            OR
            --   newer downloads
            lvc.downloaded_at > ls.scanned_at
        )
        AND
        sa.store = :store
    ORDER BY
        sa.id ASC,
        urs.created_at DESC
),
all_results AS (
    SELECT
        store_app,
        version_code_db_id,
        version_code_str,
        store_id,
        name,
        installs,
        rating_count,
        mysource,
        last_analyzed_result,
        last_scanned_at,
        last_scuccess_scanned_at
    FROM
        user_requested_apps_crawl
    UNION ALL
    SELECT
        store_app,
        version_code_db_id,
        version_code_str,
        store_id,
        name,
        installs,
        rating_count,
        mysource,
        last_analyzed_result,
        last_scanned_at,
        last_scuccess_scanned_at
    FROM
        scheduled_vcs_crawl
)
SELECT store_app,
        version_code_db_id,
        version_code_str,
        store_id,
        name,
        installs,
        rating_count,
        mysource,
        last_analyzed_result,
        last_scanned_at,
        last_scuccess_scanned_at,
        COUNT(*) OVER() AS total_queue_depth
FROM  AS all_results
LIMIT :mylimit;
