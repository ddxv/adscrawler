CREATE MATERIALIZED VIEW public.total_count_overview
TABLESPACE pg_default
AS WITH app_count AS (
    SELECT
        count(
            CASE
                WHEN sa.store = 1 THEN 1
                ELSE NULL::integer
            END
        ) AS android_apps,
        count(
            CASE
                WHEN sa.store = 2 THEN 1
                ELSE NULL::integer
            END
        ) AS ios_apps,
        count(
            CASE
                WHEN sa.store = 1 AND sa.crawl_result = 1 THEN 1
                ELSE NULL::integer
            END
        ) AS success_android_apps,
        count(
            CASE
                WHEN sa.store = 2 AND sa.crawl_result = 1 THEN 1
                ELSE NULL::integer
            END
        ) AS success_ios_apps,
        count(
            CASE
                WHEN
                    sa.store = 1
                    AND sa.updated_at >= (current_date - '7 days'::interval)
                    THEN 1
                ELSE NULL::integer
            END
        ) AS weekly_scanned_android_apps,
        count(
            CASE
                WHEN
                    sa.store = 2
                    AND sa.updated_at >= (current_date - '7 days'::interval)
                    THEN 1
                ELSE NULL::integer
            END
        ) AS weekly_scanned_ios_apps,
        count(
            CASE
                WHEN
                    sa.store = 1
                    AND sa.crawl_result = 1
                    AND sa.updated_at >= (current_date - '7 days'::interval)
                    THEN 1
                ELSE NULL::integer
            END
        ) AS weekly_success_scanned_android_apps,
        count(
            CASE
                WHEN
                    sa.store = 2
                    AND sa.crawl_result = 1
                    AND sa.updated_at >= (current_date - '7 days'::interval)
                    THEN 1
                ELSE NULL::integer
            END
        ) AS weekly_success_scanned_ios_apps
    FROM
        store_apps AS sa
),

sdk_app_count AS (
    SELECT
        count(
            DISTINCT
            CASE
                WHEN sa.store = 1 THEN vc.store_app
                ELSE NULL::integer
            END
        ) AS sdk_android_apps,
        count(
            DISTINCT
            CASE
                WHEN sa.store = 2 THEN vc.store_app
                ELSE NULL::integer
            END
        ) AS sdk_ios_apps,
        count(
            DISTINCT
            CASE
                WHEN sa.store = 1 AND vc.crawl_result = 1 THEN vc.store_app
                ELSE NULL::integer
            END
        ) AS sdk_success_android_apps,
        count(
            DISTINCT
            CASE
                WHEN sa.store = 2 AND vc.crawl_result = 1 THEN vc.store_app
                ELSE NULL::integer
            END
        ) AS sdk_success_ios_apps,
        count(
            DISTINCT
            CASE
                WHEN
                    sa.store = 1
                    AND vc.updated_at >= (current_date - '7 days'::interval)
                    AND vc.crawl_result = 1
                    THEN vc.store_app
                ELSE NULL::integer
            END
        ) AS sdk_weekly_success_android_apps,
        count(
            DISTINCT
            CASE
                WHEN
                    sa.store = 2
                    AND vc.updated_at >= (current_date - '7 days'::interval)
                    AND vc.crawl_result = 1
                    THEN vc.store_app
                ELSE NULL::integer
            END
        ) AS sdk_weekly_success_ios_apps,
        count(
            DISTINCT
            CASE
                WHEN
                    sa.store = 1
                    AND vc.updated_at >= (current_date - '7 days'::interval)
                    THEN vc.store_app
                ELSE NULL::integer
            END
        ) AS sdk_weekly_android_apps,
        count(
            DISTINCT
            CASE
                WHEN
                    sa.store = 2
                    AND vc.updated_at >= (current_date - '7 days'::interval)
                    THEN vc.store_app
                ELSE NULL::integer
            END
        ) AS sdk_weekly_ios_apps
    FROM
        version_codes AS vc
    LEFT JOIN store_apps AS sa ON
        vc.store_app = sa.id
),

appads_url_count AS (
    SELECT
        count(DISTINCT pd.url) AS appads_urls,
        count(
            DISTINCT
            CASE
                WHEN pd.crawl_result = 1 THEN pd.url
                ELSE NULL::character varying
            END
        ) AS appads_success_urls,
        count(
            DISTINCT
            CASE
                WHEN
                    pd.crawl_result = 1
                    AND pd.updated_at >= (current_date - '7 days'::interval)
                    THEN pd.url
                ELSE NULL::character varying
            END
        ) AS appads_weekly_success_urls,
        count(
            DISTINCT
            CASE
                WHEN
                    pd.updated_at >= (current_date - '7 days'::interval)
                    THEN pd.url
                ELSE NULL::character varying
            END
        ) AS appads_weekly_urls
    FROM
        pub_domains AS pd
)

SELECT
    app_count.android_apps,
    app_count.ios_apps,
    app_count.success_android_apps,
    app_count.success_ios_apps,
    app_count.weekly_scanned_android_apps,
    app_count.weekly_scanned_ios_apps,
    app_count.weekly_success_scanned_android_apps,
    app_count.weekly_success_scanned_ios_apps,
    sdk_app_count.sdk_android_apps,
    sdk_app_count.sdk_ios_apps,
    sdk_app_count.sdk_success_android_apps,
    sdk_app_count.sdk_success_ios_apps,
    sdk_app_count.sdk_weekly_success_android_apps,
    sdk_app_count.sdk_weekly_success_ios_apps,
    sdk_app_count.sdk_weekly_android_apps,
    sdk_app_count.sdk_weekly_ios_apps,
    appads_url_count.appads_urls,
    appads_url_count.appads_success_urls,
    appads_url_count.appads_weekly_success_urls,
    appads_url_count.appads_weekly_urls,
    current_date AS on_date
FROM
    app_count,
    sdk_app_count,
    appads_url_count
WITH DATA;






CREATE MATERIALIZED VIEW store_apps_rankings AS
SELECT
    ar.crawled_date,
    ar.country,
    ar.store,
    sa.store_id,
    ar.rank,
    scol.collection,
    scat.category
FROM
    app_rankings AS ar
LEFT JOIN
    store_apps AS sa
    ON
        ar.store_app = sa.id
LEFT JOIN store_collections AS scol
    ON
        ar.store_collection = scol.id
LEFT JOIN store_categories AS scat
    ON
        ar.store_category = scat.id
WHERE
    ar.crawled_date >= current_date - interval '1 year'
WITH DATA;



CREATE INDEX store_apps_rankings_idx ON
public.store_apps_rankings (store_id, crawled_date);
