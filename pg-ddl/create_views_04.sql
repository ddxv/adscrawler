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


-- DROP MATERIALIZED VIEW adtech.combined_store_apps_companies ;
-- THIS NEEDS TO BE COMBINED WITH aacsa
CREATE MATERIALIZED VIEW adtech.combined_store_apps_companies
TABLESPACE pg_default
AS WITH sdk_based_companies AS (
    SELECT
        sac.store_app,
        sac.company_id,
        sac.parent_id,
        ad.domain AS ad_domain,
        'sdk' AS tag_source
    FROM
        adtech.store_apps_companies AS sac
    LEFT JOIN adtech.company_domain_mapping AS cdm
        ON
            sac.parent_id = cdm.company_id
    LEFT JOIN ad_domains AS ad ON
        cdm.domain_id = ad.id
),

app_ads_based_companies AS (
    SELECT
        aasac.store_app,
        aasac.company_id,
        aasac.parent_id,
        aasac.ad_domain_url AS ad_domain,
        'app_ads' AS tag_source
    FROM
        adtech.app_ads_store_apps_companies AS aasac
)

SELECT *
FROM
    sdk_based_companies
UNION
SELECT *
FROM
    app_ads_based_companies
WITH DATA;

DROP INDEX IF EXISTS idx_combined_store_apps_companies;
CREATE UNIQUE INDEX idx_combined_store_apps_companies
ON adtech.combined_store_apps_companies (
    store_app, company_id, parent_id, ad_domain, tag_source
);


-- DROP MATERIALIZED VIEW adtech.companies_app_counts ;
-- A FINAL TABLE FOR company_overviews
CREATE MATERIALIZED VIEW adtech.companies_app_counts AS
WITH my_counts AS (
    SELECT DISTINCT
        csac.store_app,
        sa.store,
        csac.tag_source,
        COALESCE(
            c.name,
            csac.ad_domain
        ) AS ad_network
    FROM
        adtech.combined_store_apps_companies AS csac
    LEFT JOIN adtech.companies AS c
        ON
            csac.parent_id = c.id
    LEFT JOIN store_apps AS sa ON
        csac.store_app = sa.id
),

app_counts AS (
    SELECT
        store,
        tag_source,
        ad_network,
        COUNT(*) AS app_count
    FROM
        my_counts
    GROUP BY
        store,
        tag_source,
        ad_network
)

SELECT
    ac.app_count,
    ac.store,
    ac.tag_source,
    ac.ad_network
FROM
    app_counts AS ac
WHERE
    ac.app_count > 5
ORDER BY
    COUNT(*) DESC
WITH DATA;


DROP INDEX IF EXISTS idx_companies_app_counts;
CREATE UNIQUE INDEX idx_companies_app_counts
ON adtech.companies_app_counts (store, tag_source, ad_network);
