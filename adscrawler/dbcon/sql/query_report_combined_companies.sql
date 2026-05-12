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
store_app_sdk_strings_2025_h2 AS (
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
        cm.mapped_category AS app_category,
        cdm.company_id,
        c_1.parent_company_id AS parent_id,
        'api_call'::text AS tag_source,
        coalesce(cad_1.domain_name, saac.tld_url::character varying)
            AS ad_domain
    FROM api_calls AS saac
    LEFT JOIN store_apps AS sa_1 ON saac.store_app = sa_1.id
    LEFT JOIN
        category_mapping AS cm
        ON sa_1.category::text = cm.original_category::text
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
        cm.mapped_category AS app_category,
        sac.company_id,
        ad_1.domain_name AS ad_domain,
        'sdk'::text AS tag_source,
        coalesce(c_1.parent_company_id, sac.company_id) AS parent_id
    FROM store_app_sdk_strings_2025_h2 AS sasd
    LEFT JOIN adtech.sdks AS sac ON sasd.sdk_id = sac.id
    LEFT JOIN adtech.companies AS c_1 ON sac.company_id = c_1.id
    LEFT JOIN domains AS ad_1 ON c_1.domain_id = ad_1.id
    LEFT JOIN store_apps AS sa_1 ON sasd.store_app = sa_1.id
    LEFT JOIN
        category_mapping AS cm
        ON sa_1.category::text = cm.original_category::text
), combined_sources AS (
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
),
SELECT
    cs.ad_domain,
    cs.store_app,
    c.id AS company_id,
    coalesce(c.parent_company_id, c.id) AS parent_id,
    bool_or(cs.tag_source = 'sdk'::text) AS sdk,
    bool_or(cs.tag_source = 'api_call'::text) AS api_call,
    bool_or(cs.tag_source = 'app_ads_direct'::text) AS app_ads_direct,
    bool_or(cs.tag_source = 'app_ads_reseller'::text) AS app_ads_reseller
FROM combined_sources AS cs
LEFT JOIN domains AS ad ON cs.ad_domain::text = ad.domain_name::text
LEFT JOIN adtech.companies AS c ON ad.id = c.domain_id
GROUP BY
    cs.ad_domain, cs.store_app, c.id, c.parent_company_id;
