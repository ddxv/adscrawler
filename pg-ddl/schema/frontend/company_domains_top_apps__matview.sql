--
-- PostgreSQL database dump
--

\restrict lKt5nSOmg2n5ADLqgRB1fKdnfIKWAoSQGn8tnL5nwanyb88fxAk9ar0GYznNOTU

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
-- Name: company_domains_top_apps; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.company_domains_top_apps AS
 WITH api_data AS (
         SELECT DISTINCT saac.tld_url AS company_domain,
            c_1.name AS company_name,
            sa.store,
            sa.name,
            sa.store_id,
            sa.category AS app_category,
            sa.installs_sum_4w AS installs_d30,
            sa.icon_url_100,
            'api_call'::text AS tag_source
           FROM ((((public.api_calls saac
             LEFT JOIN public.domains d ON ((saac.tld_url = (d.domain_name)::text)))
             LEFT JOIN adtech.company_domain_mapping cdm ON ((d.id = cdm.domain_id)))
             LEFT JOIN adtech.companies c_1 ON ((cdm.company_id = c_1.id)))
             LEFT JOIN frontend.store_apps_overview sa ON ((saac.store_app = sa.id)))
        ), distinct_ad_and_pub_domains AS (
         SELECT DISTINCT pd.domain_name AS publisher_domain_url,
            ad_1.domain_name AS ad_domain_url,
            aae.relationship
           FROM ((((public.app_ads_entrys aae
             LEFT JOIN public.domains ad_1 ON ((aae.ad_domain = ad_1.id)))
             LEFT JOIN public.app_ads_map aam ON ((aae.id = aam.app_ads_entry)))
             LEFT JOIN public.domains pd ON ((aam.pub_domain = pd.id)))
             LEFT JOIN public.adstxt_crawl_results pdcr ON ((pd.id = pdcr.domain_id)))
          WHERE (((aae.relationship)::text = 'DIRECT'::text) AND ((pdcr.crawled_at - aam.updated_at) < '01:00:00'::interval))
        ), adstxt_data AS (
         SELECT DISTINCT pnv.ad_domain_url AS company_domain,
            c_1.name AS company_name,
            sa.store,
            sa.name,
            sa.store_id,
            sa.category AS app_category,
            sa.installs_sum_4w AS installs_d30,
            sa.icon_url_100,
            'app_ads_direct'::text AS tag_source
           FROM (((((public.app_urls_map aum
             LEFT JOIN public.domains pd ON ((aum.pub_domain = pd.id)))
             LEFT JOIN distinct_ad_and_pub_domains pnv ON (((pd.domain_name)::text = (pnv.publisher_domain_url)::text)))
             LEFT JOIN public.domains ad_1 ON (((pnv.ad_domain_url)::text = (ad_1.domain_name)::text)))
             LEFT JOIN adtech.companies c_1 ON ((ad_1.id = c_1.domain_id)))
             LEFT JOIN frontend.store_apps_overview sa ON ((aum.store_app = sa.id)))
          WHERE ((sa.crawl_result = 1) AND ((pnv.ad_domain_url IS NOT NULL) OR (c_1.id IS NOT NULL)))
        ), combined_sources AS (
         SELECT api_data.company_domain,
            api_data.company_name,
            api_data.store,
            api_data.name,
            api_data.store_id,
            api_data.app_category,
            api_data.installs_d30,
            api_data.icon_url_100,
            api_data.tag_source
           FROM api_data
        UNION ALL
         SELECT adstxt_data.company_domain,
            adstxt_data.company_name,
            adstxt_data.store,
            adstxt_data.name,
            adstxt_data.store_id,
            adstxt_data.app_category,
            adstxt_data.installs_d30,
            adstxt_data.icon_url_100,
            adstxt_data.tag_source
           FROM adstxt_data
        ), deduped_data AS (
         SELECT combined_sources.company_domain,
            combined_sources.company_name,
            combined_sources.store,
            combined_sources.name,
            combined_sources.store_id,
            combined_sources.app_category,
            max(combined_sources.installs_d30) AS installs_d30,
            max(combined_sources.icon_url_100) AS icon_url_100,
            bool_or((combined_sources.tag_source = 'sdk'::text)) AS sdk,
            bool_or((combined_sources.tag_source = 'api_call'::text)) AS api_call,
            bool_or((combined_sources.tag_source = 'app_ads_direct'::text)) AS app_ads_direct
           FROM combined_sources
          GROUP BY combined_sources.company_domain, combined_sources.company_name, combined_sources.store, combined_sources.name, combined_sources.store_id, combined_sources.app_category
        ), ranked_apps AS (
         SELECT deduped_data.company_domain,
            deduped_data.company_name,
            deduped_data.store,
            deduped_data.name,
            deduped_data.store_id,
            deduped_data.app_category,
            deduped_data.installs_d30,
            deduped_data.icon_url_100,
            deduped_data.sdk,
            deduped_data.api_call,
            deduped_data.app_ads_direct,
            row_number() OVER (PARTITION BY deduped_data.store, deduped_data.company_domain, deduped_data.company_name ORDER BY COALESCE((deduped_data.installs_d30)::double precision, (0)::double precision) DESC) AS app_company_rank,
            row_number() OVER (PARTITION BY deduped_data.store, deduped_data.app_category, deduped_data.company_domain, deduped_data.company_name ORDER BY COALESCE((deduped_data.installs_d30)::double precision, (0)::double precision) DESC) AS app_company_category_rank
           FROM deduped_data
        )
 SELECT company_domain,
    company_name,
    store,
    name,
    store_id,
    app_category,
    installs_d30,
    icon_url_100,
    sdk,
    api_call,
    app_ads_direct,
    app_company_rank,
    app_company_category_rank
   FROM ranked_apps
  WHERE (app_company_category_rank <= 10)
  WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.company_domains_top_apps OWNER TO postgres;

--
-- Name: idx_company_top_domains_apps; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE INDEX idx_company_top_domains_apps ON frontend.company_domains_top_apps USING btree (company_domain);


--
-- Name: idx_company_top_domains_apps_domain_rank; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE INDEX idx_company_top_domains_apps_domain_rank ON frontend.company_domains_top_apps USING btree (company_domain, app_company_rank);


--
-- Name: idx_query_company_domains_top_apps; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE INDEX idx_query_company_domains_top_apps ON frontend.company_domains_top_apps USING btree (company_domain, app_category, app_company_category_rank, store);


--
-- Name: idx_unique_company_domains_top_apps; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE UNIQUE INDEX idx_unique_company_domains_top_apps ON frontend.company_domains_top_apps USING btree (company_domain, company_name, store, name, store_id, app_category);


--
-- PostgreSQL database dump complete
--

\unrestrict lKt5nSOmg2n5ADLqgRB1fKdnfIKWAoSQGn8tnL5nwanyb88fxAk9ar0GYznNOTU

