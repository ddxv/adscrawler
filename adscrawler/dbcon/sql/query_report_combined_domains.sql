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
        ac.store_app,
        ad_1.id AS domain_id
    FROM api_calls AS ac
    LEFT JOIN domains AS ad_1 ON ac.tld_url = ad_1.domain_name::text
    WHERE
        ac.called_at >= '2025-01-01 00:00:00'::timestamp without time zone
        AND ac.called_at < :start_of_next_period
        AND ad_1.id IS NOT NULL
),
sdk_based_companies AS (
    SELECT DISTINCT
        sasd.store_app,
        c_1.domain_id
    FROM store_app_sdk_strings AS sasd
    LEFT JOIN adtech.sdks AS sd ON sasd.sdk_id = sd.id
    LEFT JOIN adtech.companies AS c_1 ON sd.company_id = c_1.id
),
distinct_ad_and_pub_domains AS (
    SELECT DISTINCT
        aam.pub_domain AS publisher_domain_id,
        aae.ad_domain AS ad_domain_id,
        aae.relationship
    FROM app_ads_entrys AS aae
    LEFT JOIN app_ads_map AS aam ON aae.id = aam.app_ads_entry
    LEFT JOIN domains AS pd ON aam.pub_domain = pd.id
    LEFT JOIN adstxt_crawl_results AS acc ON pd.id = acc.domain_id
    WHERE
        -- Earliest data in adstxt_crawl_results is 2025-10-10
        acc.crawled_at >= :start_of_period
        AND acc.created_at < :start_of_next_period
        AND (acc.crawled_at - aam.updated_at) < '01:00:00'::interval
        AND aae.updated_at >= :start_of_period
        AND aae.created_at < :start_of_next_period
),
adstxt_based_companies AS (
    SELECT DISTINCT
        aum.store_app,
        pnv.ad_domain_id AS domain_id,
        CASE
            WHEN
                pnv.relationship::text = 'DIRECT'::text
                THEN 'app_ads_direct'::text
            ELSE 'app_ads_reseller'::text
        END AS tag_source
    FROM app_urls_map AS aum
    LEFT JOIN domains AS pd ON aum.pub_domain = pd.id
    LEFT JOIN
        distinct_ad_and_pub_domains AS pnv
        ON pd.id = pnv.publisher_domain_id
    WHERE
        (pnv.ad_domain_id IS NOT NULL)
        AND aum.updated_at >= :start_of_period
        AND aum.created_at < :start_of_next_period
),
combined_sources AS (
    SELECT
        abc.domain_id,
        abc.store_app,
        'api_call' AS tag_source
    FROM api_based_companies AS abc
    UNION ALL
    SELECT
        sbc.domain_id,
        sbc.store_app,
        'sdk' AS tag_source
    FROM sdk_based_companies AS sbc
    UNION ALL
    SELECT
        atbc.domain_id,
        atbc.store_app,
        atbc.tag_source
    FROM adstxt_based_companies AS atbc
)
SELECT
    cs.domain_id,
    cs.store_app,
    bool_or(cs.tag_source = 'sdk'::text) AS sdk,
    bool_or(cs.tag_source = 'api_call'::text) AS api_call,
    bool_or(cs.tag_source = 'app_ads_direct'::text) AS app_ads_direct,
    bool_or(cs.tag_source = 'app_ads_reseller'::text) AS app_ads_reseller
FROM combined_sources AS cs
GROUP BY cs.domain_id, cs.store_app;
