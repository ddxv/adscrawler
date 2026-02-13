--
-- PostgreSQL database dump
--

\restrict cTQB8uupTzy0hzbw353JUOSj5k5HKiG2cxIf0H49RmYqSNmIIwLA7fLyCsrQcib

-- Dumped from database version 18.1 (Ubuntu 18.1-1.pgdg24.04+2)
-- Dumped by pg_dump version 18.1 (Ubuntu 18.1-1.pgdg24.04+2)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: combined_store_apps_companies; Type: MATERIALIZED VIEW; Schema: adtech; Owner: postgres
--

CREATE MATERIALIZED VIEW adtech.combined_store_apps_companies AS
 WITH api_based_companies AS (
         SELECT DISTINCT saac.store_app,
            cm.mapped_category AS app_category,
            cdm.company_id,
            c_1.parent_company_id AS parent_id,
            'api_call'::text AS tag_source,
            COALESCE(cad_1.domain_name, (saac.tld_url)::character varying) AS ad_domain
           FROM ((((((public.api_calls saac
             LEFT JOIN public.store_apps sa_1 ON ((saac.store_app = sa_1.id)))
             LEFT JOIN public.category_mapping cm ON (((sa_1.category)::text = (cm.original_category)::text)))
             LEFT JOIN public.domains ad_1 ON ((saac.tld_url = (ad_1.domain_name)::text)))
             LEFT JOIN adtech.company_domain_mapping cdm ON ((ad_1.id = cdm.domain_id)))
             LEFT JOIN adtech.companies c_1 ON ((cdm.company_id = c_1.id)))
             LEFT JOIN public.domains cad_1 ON ((c_1.domain_id = cad_1.id)))
        ), developer_based_companies AS (
         SELECT DISTINCT sa_1.id AS store_app,
            cm.mapped_category AS app_category,
            cd.company_id,
            d.domain_name AS ad_domain,
            'developer'::text AS tag_source,
            COALESCE(c_1.parent_company_id, cd.company_id) AS parent_id
           FROM ((((adtech.company_developers cd
             LEFT JOIN public.store_apps sa_1 ON ((cd.developer_id = sa_1.developer)))
             LEFT JOIN adtech.companies c_1 ON ((cd.company_id = c_1.id)))
             LEFT JOIN public.domains d ON ((c_1.domain_id = d.id)))
             LEFT JOIN public.category_mapping cm ON (((sa_1.category)::text = (cm.original_category)::text)))
        ), sdk_based_companies AS (
         SELECT DISTINCT sasd.store_app,
            cm.mapped_category AS app_category,
            sac.company_id,
            ad_1.domain_name AS ad_domain,
            'sdk'::text AS tag_source,
            COALESCE(c_1.parent_company_id, sac.company_id) AS parent_id
           FROM (((((adtech.store_app_sdk_strings sasd
             LEFT JOIN adtech.sdks sac ON ((sac.id = sasd.sdk_id)))
             LEFT JOIN adtech.companies c_1 ON ((sac.company_id = c_1.id)))
             LEFT JOIN public.domains ad_1 ON ((c_1.domain_id = ad_1.id)))
             LEFT JOIN public.store_apps sa_1 ON ((sasd.store_app = sa_1.id)))
             LEFT JOIN public.category_mapping cm ON (((sa_1.category)::text = (cm.original_category)::text)))
        ), distinct_ad_and_pub_domains AS (
         SELECT DISTINCT pd.domain_name AS publisher_domain_url,
            ad_1.domain_name AS ad_domain_url,
            aae.relationship
           FROM ((((public.app_ads_entrys aae
             LEFT JOIN public.domains ad_1 ON ((aae.ad_domain = ad_1.id)))
             LEFT JOIN public.app_ads_map aam ON ((aae.id = aam.app_ads_entry)))
             LEFT JOIN public.domains pd ON ((aam.pub_domain = pd.id)))
             LEFT JOIN public.adstxt_crawl_results pdcr ON ((pd.id = pdcr.domain_id)))
          WHERE ((pdcr.crawled_at - aam.updated_at) < '01:00:00'::interval)
        ), adstxt_based_companies AS (
         SELECT DISTINCT aum.store_app,
            cm.mapped_category AS app_category,
            c_1.id AS company_id,
            pnv.ad_domain_url AS ad_domain,
            COALESCE(c_1.parent_company_id, c_1.id) AS parent_id,
                CASE
                    WHEN ((pnv.relationship)::text = 'DIRECT'::text) THEN 'app_ads_direct'::text
                    WHEN ((pnv.relationship)::text = 'RESELLER'::text) THEN 'app_ads_reseller'::text
                    ELSE 'app_ads_unknown'::text
                END AS tag_source
           FROM ((((((public.app_urls_map aum
             LEFT JOIN public.domains pd ON ((aum.pub_domain = pd.id)))
             LEFT JOIN distinct_ad_and_pub_domains pnv ON (((pd.domain_name)::text = (pnv.publisher_domain_url)::text)))
             LEFT JOIN public.domains ad_1 ON (((pnv.ad_domain_url)::text = (ad_1.domain_name)::text)))
             LEFT JOIN adtech.companies c_1 ON ((ad_1.id = c_1.domain_id)))
             LEFT JOIN public.store_apps sa_1 ON ((aum.store_app = sa_1.id)))
             LEFT JOIN public.category_mapping cm ON (((sa_1.category)::text = (cm.original_category)::text)))
          WHERE ((sa_1.crawl_result = 1) AND ((pnv.ad_domain_url IS NOT NULL) OR (c_1.id IS NOT NULL)))
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
        UNION ALL
         SELECT adstxt_based_companies.store_app,
            adstxt_based_companies.app_category,
            adstxt_based_companies.company_id,
            adstxt_based_companies.parent_id,
            adstxt_based_companies.ad_domain,
            adstxt_based_companies.tag_source
           FROM adstxt_based_companies
        UNION ALL
         SELECT developer_based_companies.store_app,
            developer_based_companies.app_category,
            developer_based_companies.company_id,
            developer_based_companies.parent_id,
            developer_based_companies.ad_domain,
            developer_based_companies.tag_source
           FROM developer_based_companies
        )
 SELECT cs.ad_domain,
    cs.store_app,
    sa.category AS app_category,
    c.id AS company_id,
    COALESCE(c.parent_company_id, c.id) AS parent_id,
        CASE
            WHEN (sa.sdk_successful_last_crawled IS NOT NULL) THEN bool_or((cs.tag_source = 'sdk'::text))
            ELSE NULL::boolean
        END AS sdk,
        CASE
            WHEN (sa.api_successful_last_crawled IS NOT NULL) THEN bool_or((cs.tag_source = 'api_call'::text))
            ELSE NULL::boolean
        END AS api_call,
    bool_or((cs.tag_source = 'app_ads_direct'::text)) AS app_ads_direct,
    bool_or((cs.tag_source = 'app_ads_reseller'::text)) AS app_ads_reseller
   FROM (((combined_sources cs
     LEFT JOIN frontend.store_apps_overview sa ON ((cs.store_app = sa.id)))
     LEFT JOIN public.domains ad ON (((cs.ad_domain)::text = (ad.domain_name)::text)))
     LEFT JOIN adtech.companies c ON ((ad.id = c.domain_id)))
  GROUP BY cs.ad_domain, cs.store_app, sa.category, c.id, c.parent_company_id, sa.sdk_successful_last_crawled, sa.api_successful_last_crawled
  WITH NO DATA;


ALTER MATERIALIZED VIEW adtech.combined_store_apps_companies OWNER TO postgres;

--
-- Name: combined_store_app_companies_idx; Type: INDEX; Schema: adtech; Owner: postgres
--

CREATE UNIQUE INDEX combined_store_app_companies_idx ON adtech.combined_store_apps_companies USING btree (ad_domain, store_app, app_category, company_id);


--
-- PostgreSQL database dump complete
--

\unrestrict cTQB8uupTzy0hzbw353JUOSj5k5HKiG2cxIf0H49RmYqSNmIIwLA7fLyCsrQcib

