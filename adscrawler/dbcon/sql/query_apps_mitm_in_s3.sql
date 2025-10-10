WITH myresults AS (
    SELECT
        vcasr.id AS run_id,
        sa.store_id,
        sa.id AS store_app,
        vc.version_code AS version_str
    FROM
        version_code_api_scan_results AS vcasr
    LEFT JOIN version_codes AS vc
        ON
            vcasr.version_code_id = vc.id
    LEFT JOIN store_apps AS sa
        ON
            vc.store_app = sa.id
    WHERE
        sa.store = 1
        AND vcasr.run_result = 1
--    AND vc.crawl_result = 1
)
SELECT * FROM myresults;
