WITH latest_version_codes AS (
    SELECT DISTINCT ON (vc_1.store_app)
        vc_1.id,
        vc_1.store_app,
        vc_1.version_code,
        vc_1.updated_at,
        vc_1.crawl_result
    FROM version_codes AS vc_1
    INNER JOIN
        version_code_sdk_scan_results AS vcssr
        ON vc_1.id = vcssr.version_code_id
    WHERE
        vcssr.scan_result = 1
        -- The downloaded date here is used since that is when that version code is from
        -- regardless if it that version was rescanned later it would have the same or better data
        AND vc_1.created_at
        >= '2025-01-01 00:00:00'::timestamp without time zone
        AND vc_1.created_at < :start_of_next_period
    ORDER BY
        vc_1.store_app,
        (string_to_array(vc_1.version_code::text, '.'::text)::bigint []) DESC
),
store_app_sdk_strings AS (
    SELECT
        vc.store_app,
        vdm.string_id AS version_string_id,
        sd.id AS sdk_id
    FROM latest_version_codes AS vc
    INNER JOIN version_details_map AS vdm ON vc.id = vdm.version_code
    INNER JOIN
        adtech.sdk_strings AS css
        ON vdm.string_id = css.version_string_id
    INNER JOIN adtech.sdks AS sd ON css.sdk_id = sd.id
),
api_based_companies AS (
    SELECT DISTINCT
        saac.store_app,
        cdm.company_id,
        c_1.parent_company_id AS parent_id,
        'api_call'::text AS tag_source,
        coalesce(cad_1.domain_name, saac.tld_url::character varying)
            AS ad_domain
    FROM api_calls AS saac
    LEFT JOIN domains AS ad_1 ON saac.tld_url = ad_1.domain_name::text
    LEFT JOIN adtech.company_domain_mapping AS cdm ON ad_1.id = cdm.domain_id
    LEFT JOIN adtech.companies AS c_1 ON cdm.company_id = c_1.id
    LEFT JOIN domains AS cad_1 ON c_1.domain_id = cad_1.id
    WHERE
        saac.called_at >= '2025-01-01 00:00:00'::timestamp without time zone
        AND saac.called_at < :start_of_next_period
), sdk_based_companies AS (
    SELECT DISTINCT
        sasd.store_app,
        sac.company_id,
        ad_1.domain_name AS ad_domain,
        'sdk'::text AS tag_source,
        coalesce(c_1.parent_company_id, sac.company_id) AS parent_id
    FROM store_app_sdk_strings AS sasd
    LEFT JOIN adtech.sdks AS sac ON sasd.sdk_id = sac.id
    LEFT JOIN adtech.companies AS c_1 ON sac.company_id = c_1.id
    LEFT JOIN domains AS ad_1 ON c_1.domain_id = ad_1.id
),
distinct_ad_and_pub_domains AS (
    SELECT DISTINCT
        pd.domain_name AS publisher_domain_url,
        ad_1.domain_name AS ad_domain_url,
        aae.relationship
    FROM app_ads_entrys AS aae
    LEFT JOIN domains AS ad_1 ON aae.ad_domain = ad_1.id
    LEFT JOIN app_ads_map AS aam ON aae.id = aam.app_ads_entry
    LEFT JOIN domains AS pd ON aam.pub_domain = pd.id
    LEFT JOIN adstxt_crawl_results AS acc ON pd.id = acc.domain_id
    WHERE
        -- Earliest data in adstxt_crawl_results is 2025-10-10
        acc.crawled_at >= :start_of_period
        AND acc.created_at < :start_of_next_period
        AND (acc.crawled_at - aam.updated_at) < '01:00:00'::interval
        AND aae.relationship = 'DIRECT'
), adstxt_based_companies AS (
    SELECT DISTINCT
        aum.store_app,
        c_1.id AS company_id,
        pnv.ad_domain_url AS ad_domain,
        'app_ads_direct'::text AS tag_source,
        coalesce(c_1.parent_company_id, c_1.id) AS parent_id
    FROM app_urls_map AS aum
    LEFT JOIN domains AS pd ON aum.pub_domain = pd.id
    LEFT JOIN
        distinct_ad_and_pub_domains AS pnv
        ON pd.domain_name::text = pnv.publisher_domain_url::text
    LEFT JOIN
        domains AS ad_1
        ON pnv.ad_domain_url::text = ad_1.domain_name::text
    LEFT JOIN adtech.company_domain_mapping AS cdm ON ad_1.id = cdm.company_id
    LEFT JOIN adtech.companies AS c_1 ON cdm.company_id = c_1.id
    WHERE
        (pnv.ad_domain_url IS NOT NULL OR c_1.id IS NOT NULL)
        AND aum.updated_at >= :start_of_period
        AND aum.created_at < :start_of_next_period
),
combined_sources AS (
    SELECT
        api_based_companies.ad_domain,
        api_based_companies.store_app,
        api_based_companies.company_id,
        api_based_companies.parent_id,
        api_based_companies.tag_source
    FROM api_based_companies
    UNION ALL
    SELECT
        sdk_based_companies.ad_domain,
        sdk_based_companies.store_app,
        sdk_based_companies.company_id,
        sdk_based_companies.parent_id,
        sdk_based_companies.tag_source
    FROM sdk_based_companies
    UNION ALL
    SELECT
        adstxt_based_companies.ad_domain,
        adstxt_based_companies.store_app,
        adstxt_based_companies.company_id,
        adstxt_based_companies.parent_id,
        adstxt_based_companies.tag_source
    FROM adstxt_based_companies
)
SELECT
    cs.ad_domain,
    cs.store_app,
    cs.company_id,
    cs.parent_id,
    bool_or(cs.tag_source = 'sdk'::text) AS sdk,
    bool_or(cs.tag_source = 'api_call'::text) AS api_call,
    bool_or(cs.tag_source = 'app_ads_direct'::text) AS app_ads_direct
FROM combined_sources AS cs
GROUP BY
    cs.ad_domain, cs.store_app, cs.company_id, cs.parent_id;
