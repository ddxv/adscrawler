--
-- PostgreSQL database dump
--

\restrict 4twZDnZhe4eGuoKZydVdaFw1KRpIs0Jsmp38DgLmh4z5B7aSsBwK4HGLTt8ilDv

-- Dumped from database version 18.3 (Ubuntu 18.3-1.pgdg24.04+1)
-- Dumped by pg_dump version 18.3 (Ubuntu 18.3-1.pgdg24.04+1)

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
-- Name: combined_app_companies; Type: MATERIALIZED VIEW; Schema: adtech; Owner: postgres
--

CREATE MATERIALIZED VIEW adtech.combined_app_companies AS
 WITH api_based_companies AS (
         SELECT DISTINCT saac.store_app,
            ad_1.id AS domain_id
           FROM ((public.api_calls saac
             LEFT JOIN public.store_apps sa_1 ON ((saac.store_app = sa_1.id)))
             LEFT JOIN public.domains ad_1 ON ((saac.tld_url = (ad_1.domain_name)::text)))
        ), sdk_based_companies AS (
         SELECT DISTINCT sasd.store_app,
            c_1.domain_id
           FROM ((adtech.store_app_sdk_strings sasd
             LEFT JOIN adtech.sdks sac ON ((sac.id = sasd.sdk_id)))
             LEFT JOIN adtech.companies c_1 ON ((sac.company_id = c_1.id)))
        ), distinct_ad_and_pub_domains AS (
         SELECT DISTINCT pd.id AS publisher_domain_id,
            aae.ad_domain AS ad_domain_id,
            aae.relationship
           FROM (((public.app_ads_entrys aae
             LEFT JOIN public.app_ads_map aam ON ((aae.id = aam.app_ads_entry)))
             LEFT JOIN public.domains pd ON ((aam.pub_domain = pd.id)))
             LEFT JOIN public.adstxt_crawl_results pdcr ON ((pd.id = pdcr.domain_id)))
          WHERE ((pdcr.crawled_at - aam.updated_at) < '01:00:00'::interval)
        ), adstxt_based_companies AS (
         SELECT DISTINCT aum.store_app,
            pnv.ad_domain_id AS domain_id,
                CASE
                    WHEN ((pnv.relationship)::text = 'DIRECT'::text) THEN 'app_ads_direct'::text
                    WHEN ((pnv.relationship)::text = 'RESELLER'::text) THEN 'app_ads_reseller'::text
                    ELSE 'app_ads_unknown'::text
                END AS tag_source
           FROM (public.app_urls_map aum
             LEFT JOIN distinct_ad_and_pub_domains pnv ON ((aum.pub_domain = pnv.publisher_domain_id)))
          WHERE ((aum.updated_at >= (CURRENT_DATE - '90 days'::interval)) AND (pnv.ad_domain_id IS NOT NULL))
        ), developer_based_domains AS (
         SELECT DISTINCT sa_1.id AS store_app,
            d.id AS domain_id
           FROM (((adtech.company_developers cd
             LEFT JOIN public.store_apps sa_1 ON ((cd.developer_id = sa_1.developer)))
             LEFT JOIN adtech.companies c_1 ON ((cd.company_id = c_1.id)))
             LEFT JOIN public.domains d ON ((c_1.domain_id = d.id)))
        ), store_urls AS (
         SELECT dsa.store_app,
            dsa.domain_id
           FROM (public.developer_store_apps dsa
             LEFT JOIN public.domains d ON ((dsa.domain_id = d.id)))
          WHERE ((dsa.domain_id IS NOT NULL) AND (NOT (dsa.domain_id IN ( SELECT domains_third_party.domain_id
                   FROM public.domains_third_party))))
        ), app_domain_publishers AS (
         SELECT store_urls.store_app,
            store_urls.domain_id
           FROM store_urls
        UNION ALL
         SELECT developer_based_domains.store_app,
            developer_based_domains.domain_id
           FROM developer_based_domains
        ), combined_sources AS (
         SELECT api_based_companies.store_app,
            api_based_companies.domain_id,
            'api_call'::text AS tag_source
           FROM api_based_companies
        UNION ALL
         SELECT sdk_based_companies.store_app,
            sdk_based_companies.domain_id,
            'sdk'::text AS tag_source
           FROM sdk_based_companies
        UNION ALL
         SELECT adstxt_based_companies.store_app,
            adstxt_based_companies.domain_id,
            adstxt_based_companies.tag_source
           FROM adstxt_based_companies
        UNION ALL
         SELECT app_domain_publishers.store_app,
            app_domain_publishers.domain_id,
            'publisher'::text AS text
           FROM app_domain_publishers
        ), all_bools AS (
         SELECT cs.domain_id,
            cs.store_app,
                CASE
                    WHEN (sa.sdk_successful_last_crawled IS NOT NULL) THEN bool_or((cs.tag_source = 'sdk'::text))
                    ELSE NULL::boolean
                END AS sdk,
                CASE
                    WHEN (sa.api_successful_last_crawled IS NOT NULL) THEN bool_or((cs.tag_source = 'api_call'::text))
                    ELSE NULL::boolean
                END AS api_call,
            bool_or((cs.tag_source = 'publisher'::text)) AS publisher,
            bool_or((cs.tag_source = 'app_ads_direct'::text)) AS app_ads_direct,
            bool_or((cs.tag_source = 'app_ads_reseller'::text)) AS app_ads_reseller
           FROM (combined_sources cs
             LEFT JOIN frontend.store_apps_overview sa ON ((cs.store_app = sa.id)))
          GROUP BY cs.domain_id, cs.store_app, sa.sdk_successful_last_crawled, sa.api_successful_last_crawled
        )
 SELECT ab.domain_id,
    ab.store_app,
    cdm.company_id,
    ab.sdk,
    ab.api_call,
    ab.publisher,
    ab.app_ads_direct,
    ab.app_ads_reseller
   FROM (all_bools ab
     LEFT JOIN adtech.company_domain_mapping cdm ON ((ab.domain_id = cdm.domain_id)))
  WITH NO DATA;


ALTER MATERIALIZED VIEW adtech.combined_app_companies OWNER TO postgres;

--
-- PostgreSQL database dump complete
--

\unrestrict 4twZDnZhe4eGuoKZydVdaFw1KRpIs0Jsmp38DgLmh4z5B7aSsBwK4HGLTt8ilDv

