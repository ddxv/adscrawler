--
-- PostgreSQL database dump
--

\restrict SbM9K1DRK66w2ywZFT5IXPdA6XhQSPNXE48EFeuZCMj5OBSyTEleVsr52Q55gfg

-- Dumped from database version 18.4 (Ubuntu 18.4-1.pgdg26.04+1)
-- Dumped by pg_dump version 18.4 (Ubuntu 18.4-1.pgdg26.04+1)

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
-- Name: company_top_apps; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.company_top_apps AS
 WITH deduped_data AS (
         SELECT COALESCE(cd.domain_name, ad.domain_name) AS company_domain,
            sa.store,
            sa.name,
            sa.store_id,
            sa.category AS app_category,
            sa.developer_name,
            sa.icon_64,
            sa.is_removed,
            sa.country_id,
            sa.installs_sum_4w AS installs_d30,
            bool_or(cac.sdk) AS sdk,
            bool_or(cac.api_call) AS api_call,
            bool_or(cac.publisher) AS publisher,
            bool_or(cac.app_ads_direct) AS app_ads_direct
           FROM ((((adtech.combined_app_companies cac
             LEFT JOIN frontend.store_apps_overview sa ON ((cac.store_app = sa.id)))
             LEFT JOIN public.domains ad ON ((cac.domain_id = ad.id)))
             LEFT JOIN adtech.companies c_1 ON ((cac.company_id = c_1.id)))
             LEFT JOIN public.domains cd ON ((c_1.domain_id = cd.id)))
          WHERE (cac.app_ads_direct OR cac.sdk OR cac.api_call OR cac.publisher)
          GROUP BY COALESCE(cd.domain_name, ad.domain_name), sa.store, sa.name, sa.store_id, sa.category, sa.developer_name, sa.icon_64, sa.is_removed, sa.country_id, sa.installs_sum_4w
        ), ranked_apps AS (
         SELECT dd.company_domain,
            dd.store,
            dd.name,
            dd.store_id,
            dd.developer_name,
            dd.icon_64,
            dd.is_removed,
            dd.country_id,
            dd.app_category,
            dd.installs_d30,
            dd.sdk,
            dd.api_call,
            dd.publisher,
            dd.app_ads_direct,
            row_number() OVER (PARTITION BY dd.store, dd.company_domain ORDER BY (COALESCE((dd.sdk)::integer, 0) + COALESCE((dd.api_call)::integer, 0)) DESC, COALESCE((dd.installs_d30)::double precision, (0)::double precision) DESC) AS app_company_rank,
            row_number() OVER (PARTITION BY dd.store, dd.app_category, dd.company_domain ORDER BY (COALESCE((dd.sdk)::integer, 0) + COALESCE((dd.api_call)::integer, 0)) DESC, COALESCE((dd.installs_d30)::double precision, (0)::double precision) DESC) AS app_company_category_rank
           FROM deduped_data dd
        )
 SELECT ra.company_domain,
    ra.store,
    ra.name,
    ra.store_id,
    ra.developer_name,
    ra.icon_64,
    ra.is_removed,
    c.alpha2 AS country,
    ra.app_category,
    ra.installs_d30,
    ra.sdk,
    ra.api_call,
    ra.publisher,
    ra.app_ads_direct,
    ra.app_company_rank,
    ra.app_company_category_rank
   FROM (ranked_apps ra
     LEFT JOIN public.countries c ON ((ra.country_id = c.id)))
  WHERE (ra.app_company_category_rank <= 20)
  WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.company_top_apps OWNER TO postgres;

--
-- Name: idx_company_top_apps; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE INDEX idx_company_top_apps ON frontend.company_top_apps USING btree (company_domain);


--
-- Name: idx_company_top_apps_domain_rank; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE INDEX idx_company_top_apps_domain_rank ON frontend.company_top_apps USING btree (company_domain, app_company_rank);


--
-- Name: idx_query_company_top_apps; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE INDEX idx_query_company_top_apps ON frontend.company_top_apps USING btree (company_domain, app_category, app_company_category_rank, store);


--
-- Name: idx_unique_company_top_apps; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE UNIQUE INDEX idx_unique_company_top_apps ON frontend.company_top_apps USING btree (company_domain, store, name, store_id, app_category);


--
-- PostgreSQL database dump complete
--

\unrestrict SbM9K1DRK66w2ywZFT5IXPdA6XhQSPNXE48EFeuZCMj5OBSyTEleVsr52Q55gfg

