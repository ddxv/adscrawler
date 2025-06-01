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
            last_scanned AS (
                    SELECT
                        DISTINCT ON
                        (vc.store_app) *
                    FROM
                       version_code_api_scan_results vasr
                    LEFT JOIN version_codes vc 
                        ON vasr.version_code_id = vc.id
                    WHERE
                        vc.updated_at >= '2025-05-01'
                    ORDER BY vc.store_app, vasr.run_at desc
                ),
            last_successful_scanned AS (
                    SELECT
                        DISTINCT ON
                        (vc.store_app) *
                    FROM
                       version_code_api_scan_results vasr
                    LEFT JOIN version_codes vc 
                        ON vasr.version_code_id = vc.id
                    WHERE
                        vc.crawl_result = 1
                        AND vc.updated_at >= '2025-05-01'
                    ORDER BY vc.store_app, vasr.run_at desc
                ),    
            failed_runs AS (
            SELECT store_app, count(*) AS failed_attempts FROM logging.version_code_api_scan_results
            WHERE crawl_result != 1
                AND updated_at >= current_date - INTERVAL '10 days'
                GROUP BY store_app
),
        scheduled_to_run AS (
                SELECT lvc.store_app as store_app,
                sa.name,
                sa.store_id,
                sa.installs,
                ls.run_at AS last_run_at,
                fr.failed_attempts,
                ls.run_result AS last_run_result,
                lss.run_at AS last_succesful_run_at,
                lvc.last_downloaded_at
                FROM
                    latest_version_codes lvc
                LEFT JOIN last_scanned ls ON
                    lvc.store_app = ls.store_app
                LEFT JOIN last_successful_scanned lss ON lvc.id = lss.version_code_id
                LEFT JOIN store_apps sa ON
                    lvc.store_app = sa.id
                LEFT JOIN failed_runs fr ON sa.id = fr.store_app
                WHERE (ls.run_at <= CURRENT_DATE - INTERVAL '120 days' OR ls.run_at IS NULL)
                    AND sa.store = :store
                      AND (fr.failed_attempts < 1 OR fr.failed_attempts IS NULL )
                ORDER BY sa.installs DESC
                ),     
        user_requested_apps_crawl AS (
            SELECT
                DISTINCT ON (sa.id) 
                sa.id AS store_app,
                sa.store_id,
                sa.name,
                sa.installs,
                ls.run_at AS last_run_at,
                fr.failed_attempts,
                ls.run_result AS last_run_result,
                lss.run_at AS last_succesful_run_at,
                lsvc.last_downloaded_at,
                urs.created_at AS user_requested_at
            FROM
                user_requested_scan urs
            LEFT JOIN store_apps sa ON
                urs.store_id = sa.store_id
            INNER JOIN latest_version_codes lsvc ON
                sa.id = lsvc.store_app
            LEFT JOIN last_scanned ls ON lsvc.id = ls.version_code_id
            LEFT JOIN last_successful_scanned lss ON lsvc.id = lss.version_code_id
            LEFT JOIN failed_runs fr ON sa.id = fr.store_app
            WHERE
                (ls.run_at < urs.created_at
                    OR ls.run_at IS NULL
                )
                AND sa.store = :store
                AND (fr.failed_attempts < 1 OR fr.failed_attempts IS NULL 
                )
            ORDER BY sa.id, urs.created_at desc
            )
            SELECT 
                store_app,
                store_id,
                name,
                installs,
                last_run_at,
                failed_attempts,
                last_run_result,
                last_succesful_run_at,
                last_downloaded_at,
                user_requested_at,
                'user' AS mysource
                FROM user_requested_apps_crawl urac
            UNION ALL
            SELECT 
                store_app,
                store_id,
                name,
                installs,
                last_run_at,
                last_run_result,
                failed_attempts,
                last_succesful_run_at,
                last_downloaded_at,
                NULL AS user_requested_at,
                'scheduled' AS mysource
                FROM scheduled_to_run
                WHERE store_id NOT IN (SELECT store_id FROM user_requested_apps_crawl)
            ORDER BY mysource DESC, user_requested_at, installs DESC 
            LIMIT :mylimit
            ;