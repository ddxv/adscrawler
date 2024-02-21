CREATE MATERIALIZED VIEW adtech.store_apps_companies AS
WITH latest_version_codes AS (
    SELECT
        id,
        store_app,
        MAX(version_code) AS version_code
    FROM
        public.version_codes
    WHERE
        crawl_result = 1
    GROUP BY
        id,
        store_app
),

apps_with_companies AS (
    SELECT DISTINCT
        vc.store_app,
        tm.company_id
    FROM
        latest_version_codes AS vc
    LEFT JOIN public.version_details AS vd
        ON
            vc.id = vd.version_code
    INNER JOIN adtech.sdk_packages AS tm ON
        vd.android_name ILIKE tm.package_pattern || '%'
)

SELECT
    awc.store_app,
    awc.company_id
FROM
    apps_with_companies AS awc
UNION ALL
SELECT
    vc.store_app,
    -- Note: 10 is tracker db id for no network found
    10 AS company_id
FROM
    latest_version_codes AS vc
WHERE
    vc.store_app NOT IN (
        SELECT store_app
        FROM
            apps_with_companies
    )
WITH DATA;


DROP INDEX IF EXISTS idx_store_apps_companies;
CREATE UNIQUE INDEX idx_store_apps_companies
ON adtech.store_apps_companies (store_app, company_id);
