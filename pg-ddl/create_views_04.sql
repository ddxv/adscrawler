CREATE MATERIALIZED VIEW adtech.combined_store_apps_companies
TABLESPACE pg_default
AS WITH sdk_based_companies AS (
    SELECT
        sac.store_app,
        cm.mapped_category AS app_category,
        sac.company_id,
        sac.parent_id,
        ad.domain AS ad_domain,
        'sdk'::text AS tag_source
    FROM adtech.store_apps_companies_sdk AS sac
    LEFT JOIN
        adtech.company_domain_mapping AS cdm
        ON COALESCE(sac.company_id, sac.parent_id) = cdm.company_id
    LEFT JOIN ad_domains AS ad ON cdm.domain_id = ad.id
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
    LEFT JOIN adtech.company_domain_mapping AS cdm ON ad.id = cdm.domain_id
    LEFT JOIN adtech.companies AS c ON cdm.company_id = c.id
    LEFT JOIN store_apps AS sa ON aum.store_app = sa.id
    LEFT JOIN
        category_mapping AS cm
        ON sa.category::text = cm.original_category::text
    WHERE
        sa.crawl_result = 1
        AND (pnv.ad_domain_url IS NOT NULL OR c.id IS NOT NULL)
)

SELECT
    sdk_based_companies.store_app,
    sdk_based_companies.app_category,
    sdk_based_companies.company_id,
    sdk_based_companies.parent_id,
    sdk_based_companies.ad_domain,
    sdk_based_companies.tag_source
FROM sdk_based_companies
UNION
SELECT
    adstxt_based_companies.store_app,
    adstxt_based_companies.app_category,
    adstxt_based_companies.company_id,
    adstxt_based_companies.parent_id,
    adstxt_based_companies.ad_domain,
    adstxt_based_companies.tag_source
FROM adstxt_based_companies
WITH DATA;


-- adtech.combined_store_apps_parent_companies source

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
LEFT JOIN adtech.company_domain_mapping AS cdm ON c.id = cdm.company_id
LEFT JOIN ad_domains AS ad ON cdm.domain_id = ad.id
WHERE
    csac.parent_id IN (
        SELECT DISTINCT pc.id
        FROM
            adtech.companies AS pc
        LEFT JOIN adtech.companies AS c
            ON
                pc.id = c.parent_company_id
        WHERE
            c.id IS NOT NULL
    )
WITH DATA;




-- Used for the crawler when checking which apps to download
-- This is called very often, hence the need for an MV
CREATE MATERIALIZED VIEW public.store_apps_in_latest_rankings
TABLESPACE pg_default
AS SELECT DISTINCT ON (ar.store_app)
    ar.store_app,
    sa.store,
    sa.name,
    sa.installs,
    sa.rating_count,
    sa.store_id
FROM app_rankings AS ar
LEFT JOIN store_apps AS sa ON ar.store_app = sa.id
WHERE ar.crawled_date > (CURRENT_DATE - '15 days'::interval)
WITH DATA;
