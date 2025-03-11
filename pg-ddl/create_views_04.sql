-- DROP MATERIALIZED VIEW adtech.combined_store_apps_companies CASCADE ;
CREATE MATERIALIZED VIEW adtech.combined_store_apps_companies
TABLESPACE pg_default
AS
WITH sdk_based_companies AS (
    SELECT
        sac.store_app,
        cm.mapped_category AS app_category,
        sac.company_id,
        sac.parent_id,
        ad.domain AS ad_domain,
        'sdk' AS tag_source
    FROM
        adtech.store_apps_companies_sdk AS sac
    LEFT JOIN adtech.company_domain_mapping AS cdm
        ON
            COALESCE(
                sac.company_id,
                sac.parent_id
            ) = cdm.company_id
    LEFT JOIN ad_domains AS ad
        ON
            cdm.domain_id = ad.id
    LEFT JOIN store_apps AS sa
        ON
            sac.store_app = sa.id
    LEFT JOIN category_mapping AS cm ON
        sa.category = cm.original_category
),

distinct_ad_and_pub_domains AS (
    SELECT DISTINCT
        pd.url AS publisher_domain_url,
        ad.domain AS ad_domain_url,
        aae.relationship
    FROM
        app_ads_entrys AS aae
    LEFT JOIN ad_domains AS ad
        ON
            aae.ad_domain = ad.id
    LEFT JOIN app_ads_map AS aam
        ON
            aae.id = aam.app_ads_entry
    LEFT JOIN pub_domains AS pd
        ON
            aam.pub_domain = pd.id
    WHERE
        pd.crawled_at - aam.updated_at < INTERVAL '1 hour'
),

adstxt_based_companies AS (
    SELECT DISTINCT
        aum.store_app,
        cm.mapped_category AS app_category,
        c.id AS company_id,
        COALESCE(
            c.parent_company_id,
            c.id
        ) AS parent_id,
        pnv.ad_domain_url AS ad_domain,
        CASE
            WHEN pnv.relationship = 'DIRECT' THEN 'app_ads_direct'
            WHEN pnv.relationship = 'RESELLER' THEN 'app_ads_reseller'
            ELSE 'app_ads_unknown'
        END AS tag_source
    FROM
        app_urls_map AS aum
    LEFT JOIN pub_domains AS pd
        ON
            aum.pub_domain = pd.id
    LEFT JOIN distinct_ad_and_pub_domains AS pnv
        ON
            pd.url = pnv.publisher_domain_url
    LEFT JOIN ad_domains AS ad
        ON
            pnv.ad_domain_url = ad.domain
    LEFT JOIN adtech.company_domain_mapping AS cdm
        ON
            ad.id = cdm.domain_id
    LEFT JOIN adtech.companies AS c
        ON
            cdm.company_id = c.id
    LEFT JOIN store_apps AS sa
        ON
            aum.store_app = sa.id
    LEFT JOIN category_mapping AS cm
        ON
            sa.category = cm.original_category
    WHERE
        sa.crawl_result = 1
        AND (
            pnv.ad_domain_url IS NOT NULL
            OR c.id IS NOT NULL
        )
)

SELECT *
FROM
    sdk_based_companies
UNION
SELECT *
FROM
    adstxt_based_companies
WITH DATA;
