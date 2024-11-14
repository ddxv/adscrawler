CREATE MATERIALIZED VIEW total_count_overview AS
WITH app_count AS (
    SELECT
        COUNT(CASE WHEN store = 1 THEN 1 END) AS android_apps,
        COUNT(CASE WHEN store = 2 THEN 1 END) AS ios_apps,
        COUNT(CASE WHEN store = 1 AND crawl_result = 1 THEN 1 END) AS success_android_apps,
        COUNT(CASE WHEN store = 2 AND crawl_result = 1 THEN 1 END) AS success_ios_apps,
        COUNT(CASE WHEN store = 1 AND sa.updated_at >= CURRENT_DATE - INTERVAL '7 days' THEN 1 END) AS weekly_scanned_android_apps,
        COUNT(CASE WHEN store = 2 AND sa.updated_at >= CURRENT_DATE - INTERVAL '7 days' THEN 1 END) AS weekly_scanned_ios_apps
    FROM
        store_apps sa
),
sdk_app_count AS (
    SELECT
        COUNT(DISTINCT CASE WHEN sa.store = 1 THEN vc.store_app END) AS sdk_android_apps,
        COUNT(DISTINCT CASE WHEN sa.store = 2 THEN vc.store_app END) AS sdk_ios_apps,
        COUNT(DISTINCT CASE WHEN sa.store = 1 AND vc.updated_at >= CURRENT_DATE - INTERVAL '7 days' AND vc.crawl_result = 1 THEN vc.store_app END) AS sdk_weekly_success_android_apps,
        COUNT(DISTINCT CASE WHEN sa.store = 2 AND vc.updated_at >= CURRENT_DATE - INTERVAL '7 days' AND vc.crawl_result = 1 THEN vc.store_app END) AS sdk_weekly_success_ios_apps,
        COUNT(DISTINCT CASE WHEN sa.store = 1 AND vc.updated_at >= CURRENT_DATE - INTERVAL '7 days' THEN vc.store_app END) AS sdk_weekly_android_apps,
        COUNT(DISTINCT CASE WHEN sa.store = 2 AND vc.updated_at >= CURRENT_DATE - INTERVAL '7 days' THEN vc.store_app END) AS sdk_weekly_ios_apps
    FROM
        version_codes vc
    LEFT JOIN store_apps sa ON
        vc.store_app = sa.id
),
appads_url_count AS (
    SELECT
        COUNT(DISTINCT url) AS appads_urls,
        count(DISTINCT CASE WHEN pd.crawl_result = 1 THEN pd.url END) appads_success_urls,
        count(DISTINCT CASE WHEN pd.crawl_result = 1 AND pd.updated_at >= CURRENT_DATE - INTERVAL '7 days' THEN pd.url END) appads_weekly_success_urls,
        count(DISTINCT CASE WHEN pd.updated_at >= CURRENT_DATE - INTERVAL '7 days' THEN pd.url END) appads_weekly_urls
    FROM
        pub_domains pd
)
SELECT
    CURRENT_DATE AS on_date,
    app_count.android_apps,
    app_count.ios_apps,
    app_count.success_android_apps,
    app_count.success_ios_apps,
    sdk_app_count.sdk_android_apps,
    sdk_app_count.sdk_ios_apps,
    sdk_app_count.sdk_weekly_success_android_apps,
    sdk_app_count.sdk_weekly_success_ios_apps,
    sdk_app_count.sdk_weekly_android_apps,
    sdk_app_count.sdk_weekly_ios_apps,
    appads_url_count.appads_urls,
    appads_url_count.appads_success_urls,
    appads_url_count.appads_weekly_success_urls,
    appads_url_count.appads_weekly_urls
FROM
    app_count,
    sdk_app_count,
    appads_url_count
;


CREATE MATERIALIZED VIEW companies_apps_version_details AS
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
)
 SELECT
    DISTINCT
        vd.xml_path,
        vd.tag,
        vd.value_name,
        sa.store,
        vc.store_app,
        tm.company_id,
        COALESCE(
            pc.parent_company_id,
            tm.company_id
    ) AS parent_id
FROM
        latest_version_codes AS vc
LEFT JOIN public.version_details AS vd
        ON
            vc.id = vd.version_code
LEFT JOIN adtech.sdk_packages AS tm
        ON
            vd.value_name ILIKE tm.package_pattern || '%'
LEFT JOIN adtech.companies AS pc ON
        tm.company_id = pc.id
LEFT JOIN store_apps sa ON vc.store_app = sa.id
;


CREATE MATERIALIZED VIEW companies_version_details_count AS
SELECT
    cavd.store,
    c.name AS company_name,
    ad."domain" AS company_domain,
    cavd.xml_path,
    cavd.value_name,
    count(DISTINCT store_app) AS app_count
FROM
    companies_apps_version_details cavd
LEFT JOIN adtech.companies c ON
    cavd.company_id = c.id
LEFT JOIN adtech.company_domain_mapping cdm ON
    cavd.company_id = cdm.company_id
LEFT JOIN ad_domains ad ON
    cdm.domain_id = ad.id
GROUP BY
    cavd.store,
    c.name,
    ad."domain",
    cavd.xml_path,
    cavd.value_name
;