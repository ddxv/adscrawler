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
        count(DISTINCT CASE WHEN pd.created_at >= CURRENT_DATE - INTERVAL '7 days' THEN pd.url END) appads_weekly_urls
    FROM
        pub_domains pd
)
SELECT
    CURRENT_DATE AS date,
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