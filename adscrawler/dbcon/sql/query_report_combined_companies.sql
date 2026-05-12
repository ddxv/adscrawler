WITH latest_version_codes AS (
         SELECT DISTINCT ON (vc_1.store_app) vc_1.id,
            vc_1.store_app,
            vc_1.version_code,
            vc_1.updated_at,
            vc_1.crawl_result
           FROM version_codes vc_1
             JOIN version_code_sdk_scan_results vcssr ON vc_1.id = vcssr.version_code_id
          WHERE vcssr.scan_result = 1 
          -- The downloaded date here is used since that is when that version code is from
          -- regardless if it that version was rescanned later it would have the same or better data
          AND vc_1.created_at >= '2025-01-01 00:00:00'::timestamp without time zone AND vc_1.created_at < :first_date_of_next_period
          ORDER BY vc_1.store_app, (string_to_array(vc_1.version_code::text, '.'::text)::bigint[]) DESC
        ),
store_app_sdk_strings_2025_h2 AS ( SELECT vc.store_app,
    vdm.string_id AS version_string_id,
    sd.id AS sdk_id
   FROM latest_version_codes vc
     JOIN version_details_map vdm ON vc.id = vdm.version_code
     JOIN adtech.sdk_strings css ON vdm.string_id = css.version_string_id
     JOIN adtech.sdks sd ON css.sdk_id = sd.id
),
api_based_companies AS (
         SELECT DISTINCT saac.store_app,
            cm.mapped_category AS app_category,
            cdm.company_id,
            c_1.parent_company_id AS parent_id,
            'api_call'::text AS tag_source,
            COALESCE(cad_1.domain_name, saac.tld_url::character varying) AS ad_domain
           FROM api_calls saac
             LEFT JOIN store_apps sa_1 ON saac.store_app = sa_1.id
             LEFT JOIN category_mapping cm ON sa_1.category::text = cm.original_category::text
             LEFT JOIN domains ad_1 ON saac.tld_url = ad_1.domain_name::text
             LEFT JOIN adtech.company_domain_mapping cdm ON ad_1.id = cdm.domain_id
             LEFT JOIN adtech.companies c_1 ON cdm.company_id = c_1.id
             LEFT JOIN domains cad_1 ON c_1.domain_id = cad_1.id
          WHERE saac.called_at >= '2025-01-01 00:00:00'::timestamp without time zone AND saac.called_at < :first_date_of_next_period
        ), developer_based_companies AS (
         SELECT DISTINCT sa_1.id AS store_app,
            cm.mapped_category AS app_category,
            cd.company_id,
            d.domain_name AS ad_domain,
            'developer'::text AS tag_source,
            COALESCE(c_1.parent_company_id, cd.company_id) AS parent_id
           FROM adtech.company_developers cd
             LEFT JOIN store_apps sa_1 ON cd.developer_id = sa_1.developer
             LEFT JOIN adtech.companies c_1 ON cd.company_id = c_1.id
             LEFT JOIN domains d ON c_1.domain_id = d.id
             LEFT JOIN category_mapping cm ON sa_1.category::text = cm.original_category::text
        ), sdk_based_companies AS (
         SELECT DISTINCT sasd.store_app,
            cm.mapped_category AS app_category,
            sac.company_id,
            ad_1.domain_name AS ad_domain,
            'sdk'::text AS tag_source,
            COALESCE(c_1.parent_company_id, sac.company_id) AS parent_id
           FROM store_app_sdk_strings_2025_h2 sasd
             LEFT JOIN adtech.sdks sac ON sac.id = sasd.sdk_id
             LEFT JOIN adtech.companies c_1 ON sac.company_id = c_1.id
             LEFT JOIN domains ad_1 ON c_1.domain_id = ad_1.id
             LEFT JOIN store_apps sa_1 ON sasd.store_app = sa_1.id
             LEFT JOIN category_mapping cm ON sa_1.category::text = cm.original_category::text
        ), distinct_ad_and_pub_domains AS (
         SELECT DISTINCT pd.domain_name AS publisher_domain_url,
            ad_1.domain_name AS ad_domain_url,
            aae.relationship
           FROM app_ads_entrys aae
             LEFT JOIN domains ad_1 ON aae.ad_domain = ad_1.id
             LEFT JOIN app_ads_map aam ON aae.id = aam.app_ads_entry
             LEFT JOIN domains pd ON aam.pub_domain = pd.id
             LEFT JOIN adstxt_crawl_results pdcr ON pd.id = pdcr.domain_id
        ), combined_sources AS (
         SELECT api_based_companies.store_app,
            api_based_companies.app_category,
            api_based_companies.company_id,
            api_based_companies.parent_id,
            api_based_companies.ad_domain,
            api_based_companies.tag_source
           FROM api_based_companies
        UNION ALL
         SELECT sdk_based_companies.store_app,
            sdk_based_companies.app_category,
            sdk_based_companies.company_id,
            sdk_based_companies.parent_id,
            sdk_based_companies.ad_domain,
            sdk_based_companies.tag_source
           FROM sdk_based_companies
        ),
SELECT cs.ad_domain,
    cs.store_app,
    c.id AS company_id,
    COALESCE(c.parent_company_id, c.id) AS parent_id,
        CASE
            WHEN sa.sdk_successful_last_crawled IS NOT NULL THEN bool_or(cs.tag_source = 'sdk'::text)
            ELSE NULL::boolean
        END AS sdk,
        CASE
            WHEN sa.api_successful_last_crawled IS NOT NULL THEN bool_or(cs.tag_source = 'api_call'::text)
            ELSE NULL::boolean
        END AS api_call,
    bool_or(cs.tag_source = 'app_ads_direct'::text) AS app_ads_direct,
    bool_or(cs.tag_source = 'app_ads_reseller'::text) AS app_ads_reseller
   FROM combined_sources cs
     LEFT JOIN domains ad ON cs.ad_domain::text = ad.domain_name::text
     LEFT JOIN adtech.companies c ON ad.id = c.domain_id
  GROUP BY cs.ad_domain, cs.store_app, c.id, c.parent_company_id, sa.sdk_successful_last_crawled, sa.api_successful_last_crawled
;