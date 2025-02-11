-- DROP MATERIALIZED VIEW adtech.app_ads_store_apps_companies;
-- store_apps that were found connected to app_ads txt files
-- PARTNER to adtech.store_apps_companies (from SDK)
CREATE MATERIALIZED VIEW adtech.app_ads_store_apps_companies
TABLESPACE pg_default
AS SELECT DISTINCT
    aum.store_app,
    sa.name AS app_name,
    aum.pub_domain,
    pd.url AS pub_domain_url,
    pnv.ad_domain_url,
    pnv.relationship,
    c.id AS company_id,
    c.name AS company_name,
    COALESCE(c.parent_company_id, c.id) AS parent_id
FROM
    app_urls_map AS aum
LEFT JOIN pub_domains AS pd
    ON
        aum.pub_domain = pd.id
LEFT JOIN publisher_network_view AS pnv
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
WHERE
    sa.crawl_result = 1 AND (pnv.ad_domain_url IS NOT NULL OR c.id IS NOT NULL)
WITH DATA;

DROP INDEX IF EXISTS idx_app_ads_store_apps_companies;
CREATE UNIQUE INDEX idx_app_ads_store_apps_companies
ON
adtech.app_ads_store_apps_companies (
    store_app,
    app_name,
    pub_domain,
    pub_domain_url,
    ad_domain_url,
    relationship,
    company_id,
    company_name,
    parent_id
);


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
        adtech.store_apps_companies AS sac
    LEFT JOIN adtech.company_domain_mapping AS cdm
        ON
            COALESCE(sac.company_id, sac.parent_id) = cdm.company_id
    LEFT JOIN ad_domains AS ad
        ON
            cdm.domain_id = ad.id
    LEFT JOIN store_apps AS sa ON sac.store_app = sa.id
    LEFT JOIN category_mapping AS cm ON sa.category = cm.original_category
),

app_ads_based_companies AS (
    SELECT
        aasac.store_app,
        cm.mapped_category AS app_category,
        aasac.company_id,
        aasac.parent_id,
        aasac.ad_domain_url AS ad_domain,
        'app_ads_direct' AS tag_source
    FROM
        adtech.app_ads_store_apps_companies AS aasac
    LEFT JOIN store_apps AS sa ON aasac.store_app = sa.id
    LEFT JOIN category_mapping AS cm ON sa.category = cm.original_category
    WHERE aasac.relationship = 'DIRECT'
),

app_ads_reseller_based_companies AS (
    SELECT
        aasac.store_app,
        cm.mapped_category AS app_category,
        aasac.company_id,
        aasac.parent_id,
        aasac.ad_domain_url AS ad_domain,
        'app_ads_reseller' AS tag_source
    FROM
        adtech.app_ads_store_apps_companies AS aasac
    LEFT JOIN store_apps AS sa ON aasac.store_app = sa.id
    LEFT JOIN category_mapping AS cm ON sa.category = cm.original_category
    WHERE aasac.relationship = 'RESELLER'
)

SELECT *
FROM
    sdk_based_companies
UNION
SELECT *
FROM
    app_ads_based_companies
UNION
SELECT *
FROM
    app_ads_reseller_based_companies
WITH DATA;





-- DROP MATERIALIZED VIEW adtech.companies_parent_categories_app_counts;
CREATE MATERIALIZED VIEW adtech.companies_parent_categories_app_counts AS
SELECT
    ad.domain AS company_domain,
    c.name AS company_name,
    csac.app_category,
    COUNT(DISTINCT csac.store_app) AS app_count
FROM
    adtech.combined_store_apps_companies AS csac
LEFT JOIN adtech.companies AS c ON csac.parent_id = c.id
LEFT JOIN adtech.company_domain_mapping AS cdm ON c.id = cdm.company_id
LEFT JOIN ad_domains AS ad ON cdm.domain_id = ad.id
GROUP BY
    ad.domain, c.name, csac.app_category
ORDER BY c.name ASC, app_count DESC
WITH DATA;

DROP INDEX IF EXISTS idx_companies_parent_categories_app_counts;
CREATE UNIQUE INDEX idx_companies_parent_categories_app_counts
ON adtech.companies_parent_categories_app_counts (
    company_domain, company_name, app_category
);
