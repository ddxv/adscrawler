
SELECT
    DISTINCT saac.store_app,
    cm.mapped_category AS app_category,
    cdm.company_id,
    c.parent_company_id AS parent_id,
    saac.tld_url AS ad_domain,
    'api_call' AS tag_source
FROM
    store_app_api_calls saac
LEFT JOIN store_apps sa ON
    saac.store_app = sa.id
LEFT JOIN
        category_mapping AS cm
        ON
    sa.category::text = cm.original_category::text
LEFT JOIN ad_domains ad ON
    saac.tld_url = ad."domain"
LEFT JOIN adtech.company_domain_mapping cdm ON
    ad.id = cdm.domain_id
LEFT JOIN adtech.companies c ON
    cdm.company_id = c.id;

    sdk_based_companies.company_id,
    sdk_based_companies.parent_id,
    sdk_based_companies.ad_domain,
    sdk_based_companies.tag_source


CREATE MATERIALIZED VIEW adtech.combined_store_apps_companies
TABLESPACE pg_default
AS WITH
api_based_companies AS (SELECT
    DISTINCT saac.store_app,
    cm.mapped_category AS app_category,
    cdm.company_id,
    c.parent_company_id AS parent_id,
    saac.tld_url AS ad_domain,
    'api_call' AS tag_source
FROM
    store_app_api_calls saac
LEFT JOIN store_apps sa ON
    saac.store_app = sa.id
LEFT JOIN
        category_mapping AS cm
        ON
    sa.category::text = cm.original_category::text
LEFT JOIN ad_domains ad ON
    saac.tld_url = ad."domain"
LEFT JOIN adtech.company_domain_mapping cdm ON
    ad.id = cdm.domain_id
LEFT JOIN adtech.companies c ON
    cdm.company_id = c.id),
sdk_based_companies AS (
    SELECT
        sac.store_app,
        cm.mapped_category AS app_category,
        sac.company_id,
        sac.parent_id,
        ad.domain AS ad_domain,
        'sdk'::text AS tag_source
    FROM adtech.store_apps_companies_sdk AS sac
    LEFT JOIN adtech.companies AS c ON sac.company_id = c.id
    LEFT JOIN ad_domains AS ad ON c.domain_id = ad.id
    LEFT JOIN store_apps AS sa ON sac.store_app = sa.id
    LEFT JOIN
        category_mapping AS cm
        ON sa.category::text = cm.original_category::text
),
distinct_ad_and_pub_domains AS (
    SELECT DISTINCT
        pd.url AS publisher_domain_url,
        ad.domain AS ad_domain_url,
        aae.relationship
    FROM app_ads_entrys AS aae
    LEFT JOIN ad_domains AS ad ON aae.ad_domain = ad.id
    LEFT JOIN app_ads_map AS aam ON aae.id = aam.app_ads_entry
    LEFT JOIN pub_domains AS pd ON aam.pub_domain = pd.id
    WHERE (pd.crawled_at - aam.updated_at) < '01:00:00'::interval
),
adstxt_based_companies AS (
    SELECT DISTINCT
        aum.store_app,
        cm.mapped_category AS app_category,
        c.id AS company_id,
        pnv.ad_domain_url AS ad_domain,
        COALESCE(c.parent_company_id, c.id) AS parent_id,
        CASE
            WHEN
                pnv.relationship::text = 'DIRECT'::text
                THEN 'app_ads_direct'::text
            WHEN
                pnv.relationship::text = 'RESELLER'::text
                THEN 'app_ads_reseller'::text
            ELSE 'app_ads_unknown'::text
        END AS tag_source
    FROM app_urls_map AS aum
    LEFT JOIN pub_domains AS pd ON aum.pub_domain = pd.id
    LEFT JOIN
        distinct_ad_and_pub_domains AS pnv
        ON pd.url::text = pnv.publisher_domain_url::text
    LEFT JOIN ad_domains AS ad ON pnv.ad_domain_url::text = ad.domain::text
    LEFT JOIN adtech.companies AS c ON ad.id = c.domain_id
    LEFT JOIN store_apps AS sa ON aum.store_app = sa.id
    LEFT JOIN
        category_mapping AS cm
        ON sa.category::text = cm.original_category::text
    WHERE
        sa.crawl_result = 1
        AND (pnv.ad_domain_url IS NOT NULL OR c.id IS NOT NULL)
),
combined_sources AS (
SELECT
    api_based_companies.store_app,
    api_based_companies.app_category,
    api_based_companies.company_id,
    api_based_companies.parent_id,
    api_based_companies.ad_domain,
    api_based_companies.tag_source
FROM api_based_companies
UNION ALL
SELECT
    sdk_based_companies.store_app,
    sdk_based_companies.app_category,
    sdk_based_companies.company_id,
    sdk_based_companies.parent_id,
    sdk_based_companies.ad_domain,
    sdk_based_companies.tag_source
FROM sdk_based_companies
UNION ALL
SELECT
    adstxt_based_companies.store_app,
    adstxt_based_companies.app_category,
    adstxt_based_companies.company_id,
    adstxt_based_companies.parent_id,
    adstxt_based_companies.ad_domain,
    adstxt_based_companies.tag_source
FROM adstxt_based_companies)
SELECT
    cs.ad_domain,
    cs.store_app,
    sa.category AS app_category,
    c.id AS company_id,
    COALESCE(c.parent_company_id, c.id) AS parent_id,
    CASE
        WHEN sa.sdk_successful_last_crawled IS NOT NULL THEN bool_or(cs.tag_source = 'sdk')
        ELSE NULL
    END AS sdk,
    CASE
        WHEN sa.api_successful_last_crawled IS NOT NULL THEN bool_or(cs.tag_source = 'api_call')
        ELSE NULL
    END AS api_call,
    bool_or(cs.tag_source = 'app_ads_direct') AS app_ads_direct,
    bool_or(cs.tag_source = 'app_ads_reseller') AS app_ads_reseller
FROM combined_sources cs
LEFT JOIN frontend.store_apps_overview sa ON cs.store_app = sa.id
LEFT JOIN ad_domains ad ON cs.ad_domain = ad.domain
LEFT JOIN adtech.companies c ON ad.id = c.domain_id
GROUP BY
    cs.ad_domain,
    cs.store_app,
    sa.category,
    c.id,
    c.parent_company_id,
    sa.sdk_successful_last_crawled,
    sa.api_successful_last_crawled
WITH DATA;


CREATE UNIQUE INDEX combined_store_app_companies_idx ON adtech.combined_store_apps_companies USING btree (ad_domain, store_app, app_category, company_id);



CREATE MATERIALIZED VIEW adtech.combined_store_apps_parent_companies
TABLESPACE pg_default
AS SELECT DISTINCT
    csac.store_app,
    csac.app_category,
    csac.parent_id AS company_id,
    csac.tag_source,
    COALESCE(ad.domain, csac.ad_domain) AS ad_domain
FROM adtech.combined_store_apps_companies AS csac
LEFT JOIN adtech.companies AS c ON csac.parent_id = c.id
LEFT JOIN ad_domains AS ad ON c.domain_id = ad.id
WHERE (csac.parent_id IN (
    SELECT DISTINCT pc.id
    FROM adtech.companies AS pc
    LEFT JOIN adtech.companies AS c_1 ON pc.id = c_1.parent_company_id
    WHERE c_1.id IS NOT NULL
))
WITH DATA;


CREATE MATERIALIZED VIEW public.store_apps_in_latest_rankings
TABLESPACE pg_default
AS
SELECT DISTINCT ON
(ar.store_app)
    ar.store_app,
    sa.store,
    sa.store_last_updated,
    sa.name,
    sa.installs,
    sa.rating_count,
    sa.store_id
FROM
    frontend.store_app_ranks_weekly AS ar
LEFT JOIN store_apps AS sa
    ON
        ar.store_app = sa.id
LEFT JOIN countries AS c
    ON
        ar.country = c.id
WHERE
    sa.free
    AND (
        ar.store_collection = ANY(
            ARRAY[
                1,
                3,
                4,
                6
            ]
        )
    )
    AND ar.crawled_date > (
        CURRENT_DATE - '15 days'::interval
    )
    AND (
        c.alpha2::text = ANY(
            ARRAY[
                'US'::character varying::text,
                'CN'::character varying::text,
                'DE'::character varying::text,
                'ID'::character varying::text,
                'IN'::character varying::text,
                'JP'::character varying::text,
                'FR'::character varying::text,
                'BR'::character varying::text,
                'MX'::character varying::text,
                'KR'::character varying::text,
                'RU'::character varying::text
            ]
        )
    )
    AND ar.rank < 150
    AND (
        sa.installs > 50000::double precision
        OR sa.rating_count > 1000
    )
WITH DATA;
