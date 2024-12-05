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


CREATE INDEX companies_apps_version_details_store_id_idx ON
public.companies_apps_version_details (store_id);



CREATE MATERIALIZED VIEW companies_apps_version_details AS
WITH latest_version_codes AS (
    SELECT
        id,
        store_app,
        max(version_code) AS version_code
    FROM
        public.version_codes
    WHERE
        crawl_result = 1
    GROUP BY
        id,
        store_app
)

SELECT DISTINCT
    vd.xml_path,
    vd.tag,
    vd.value_name,
    sa.store,
    sa.store_id,
    tm.company_id,
    c.name AS company_name,
    ad.domain AS company_domain,
    cats.url_slug AS category_slug
FROM
    latest_version_codes AS vc
LEFT JOIN public.version_details AS vd
    ON
        vc.id = vd.version_code
LEFT JOIN adtech.sdk_packages AS tm
    ON
        vd.value_name ILIKE tm.package_pattern || '%'
LEFT JOIN adtech.companies AS c
    ON
        tm.company_id = c.id
LEFT JOIN adtech.company_categories AS cc
    ON
        c.id = cc.company_id
LEFT JOIN adtech.categories AS cats
    ON
        cc.category_id = cats.id
LEFT JOIN adtech.company_domain_mapping AS cdm
    ON
        tm.company_id = cdm.company_id
LEFT JOIN ad_domains AS ad
    ON
        cdm.domain_id = ad.id
LEFT JOIN store_apps AS sa
    ON
        vc.store_app = sa.id
WITH DATA;

CREATE INDEX companies_apps_version_details_store_id_idx ON
public.companies_apps_version_details (store_id);




CREATE MATERIALIZED VIEW companies_version_details_count AS
SELECT
    cavd.store,
    cavd.company_name,
    cavd.company_domain,
    cavd.xml_path,
    cavd.value_name,
    count(DISTINCT cavd.store_id) AS app_count
FROM
    companies_apps_version_details AS cavd
GROUP BY
    cavd.store,
    cavd.company_name,
    cavd.company_domain,
    cavd.xml_path,
    cavd.value_name
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


CREATE MATERIALIZED VIEW adstxt_entries_store_apps AS
WITH parent_companies AS (
    SELECT
        c.id AS company_id,
        c.name AS company_name,
        coalesce(
            c.parent_company_id,
            c.id
        ) AS parent_company_id,
        coalesce(
            pc.name,
            c.name
        ) AS parent_company_name
    FROM
        adtech.companies AS c
    LEFT JOIN adtech.companies AS pc ON
        c.parent_company_id = pc.id
)

SELECT DISTINCT
    sa.store_id,
    d.developer_id,
    myc.parent_company_name AS company_name,
    aav.ad_domain_url,
    aav.publisher_id,
    aav.relationship,
    aav.developer_domain_crawled_at
FROM
    app_ads_view AS aav
LEFT JOIN pub_domains AS pd
    ON
        aav.developer_domain_url = pd.url
LEFT JOIN app_urls_map AS aum
    ON
        pd.id = aum.pub_domain
LEFT JOIN store_apps AS sa
    ON
        aum.store_app = sa.id
LEFT JOIN developers AS d
    ON
        sa.developer = d.id
LEFT JOIN adtech.company_domain_mapping AS cdm
    ON
        aav.ad_domain = cdm.domain_id
LEFT JOIN parent_companies AS myc
    ON
        cdm.company_id = myc.company_id
WITH DATA;


CREATE INDEX adstxt_entries_store_apps_idx ON
public.adstxt_entries_store_apps (store_id);;
