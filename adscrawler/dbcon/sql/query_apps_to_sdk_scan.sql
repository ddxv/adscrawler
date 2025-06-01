WITH latest_version_codes AS (
                SELECT
                    DISTINCT ON
                    (store_app)
                    id,
                    store_app,
                    version_code,
                    updated_at AS last_downloaded_at,
                    crawl_result AS download_result
                FROM
                    version_codes
                WHERE crawl_result = 1
                -- HACKY FIX only try for apps that have successuflly been downloaded, but this table still is all history of version_codes in general
                AND updated_at >= '2025-05-01'
                ORDER BY
                    store_app,
                    updated_at DESC,
                    string_to_array(version_code, '.')::bigint[] DESC
            ),
            last_scan AS (
                SELECT
                    DISTINCT ON
                    (vc.store_app)
                    vc.store_app,
                    version_code_id AS version_code,
                    scanned_at,
                    scan_result
                FROM
                    version_code_sdk_scan_results lsscr
                LEFT JOIN version_codes vc ON lsscr.version_code_id = vc.id
                ORDER BY
                    vc.store_app,
                    lsscr.scanned_at DESC 
            ),
            last_scan_succeed AS (
                SELECT
                    DISTINCT ON
                    (vc.store_app)
                    vc.id,
                    vc.store_app,
                    vc.version_code,
                    vcss.scanned_at,
                    vcss.scan_result
                FROM
                    version_codes vc
                LEFT JOIN version_code_sdk_scan_results vcss ON 
                vc.id = vcss.version_code_id
                WHERE vcss.scan_result = 1 
                ORDER BY
                    vc.store_app,
                    vcss.scanned_at DESC,
                    string_to_array(vc.version_code, '.')::bigint[] DESC
            ),
            scheduled_apps_crawl AS (
            SELECT
                dc.store_app,
                dc.store_id,
                dc.name,
                dc.installs,
                dc.rating_count,
                'regular' AS mysource,
                ls.scan_result AS last_analyzed_result,
                ls.scanned_at AS last_scanned_at,
                lsvc.scanned_at AS last_scuccess_scanned_at
            FROM
                store_apps_in_latest_rankings dc
            RIGHT JOIN latest_version_codes vc ON
                vc.store_app = dc.store_app
            LEFT JOIN last_scan ls ON dc.store_app = ls.store_app
            LEFT JOIN last_scan_succeed lsvc ON
                dc.store_app = lsvc.store_app
            WHERE
                dc.store = :store
                AND vc.download_result = 1
                AND
                (   
                    ls.scanned_at IS NULL 
                    OR
                        (
                        (lsvc.scan_result = 1 AND lsvc.scanned_at < current_date - INTERVAL '30 days')
                        OR
                        (lsvc.scan_result IN (2,3,4) AND (lsvc.scanned_at < current_date - INTERVAL '10 days' AND ls.scanned_at < current_date - INTERVAL '3 days'))
                        )
                )
            ORDER BY
            (CASE
                WHEN download_result IS NULL THEN 0
                ELSE 1
            END),
            GREATEST(
                    COALESCE(installs, 0),
                    COALESCE(CAST(rating_count AS bigint), 0)*50
                )
            DESC NULLS LAST
            ),
            user_requested_apps_crawl AS (
            SELECT
                DISTINCT ON (sa.id)
                sa.id AS store_app,
                sa.store_id,
                sa.name,
                sa.installs,
                sa.rating_count,
                'user' AS mysource,
                ls.scan_result AS last_analyzed_result,
                ls.scanned_at AS last_scanned_at,
                lsvc.scanned_at AS last_scuccess_scanned_at
            FROM
                user_requested_scan urs
            LEFT JOIN store_apps sa ON
                urs.store_id = sa.store_id
            LEFT JOIN last_scan ls ON sa.id = ls.store_app
            LEFT JOIN last_scan_succeed lsvc ON
                sa.id = lsvc.store_app                
            RIGHT JOIN latest_version_codes lvc ON
                sa.id = lvc.store_app
            WHERE
                (lsvc.scanned_at < urs.created_at
                    OR lsvc.scanned_at IS NULL
                )
                AND (lvc.last_downloaded_at < current_date - INTERVAL '1 days'
                    OR lvc.last_downloaded_at IS NULL)
                AND sa.store = :store
            ORDER BY sa.id, urs.created_at desc
            )
        SELECT
            store_app,
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
            store_id,
            name,
            installs,
            rating_count,
            mysource,
           last_analyzed_result,
            last_scanned_at,
            last_scuccess_scanned_at
        FROM
            scheduled_apps_crawl
            LIMIT :mylimit
        ;