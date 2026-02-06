--
-- PostgreSQL database dump
--

\restrict Fqnipgpngj6nmf3MpEwqMl2j2tcKnRTG2STP003feAKGm1uNvkQItscbrVi1KtZ

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
-- Name: company_top_apps; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.company_top_apps AS
 WITH deduped_data AS (
         SELECT cac.ad_domain AS company_domain,
            c.name AS company_name,
            sa.store,
            sa.name,
            sa.store_id,
            cac.app_category,
            sa.developer_name,
            sa.icon_url_100,
            sa.installs_sum_4w AS installs_d30,
            sa.ratings_sum_4w AS rating_count_d30,
            cac.sdk,
            cac.api_call,
            cac.app_ads_direct
           FROM ((adtech.combined_store_apps_companies cac
             LEFT JOIN frontend.store_apps_overview sa ON ((cac.store_app = sa.id)))
             LEFT JOIN adtech.companies c ON ((cac.company_id = c.id)))
          WHERE (cac.app_ads_direct OR cac.sdk OR cac.api_call)
        ), ranked_apps AS (
         SELECT deduped_data.company_domain,
            deduped_data.company_name,
            deduped_data.store,
            deduped_data.name,
            deduped_data.store_id,
            deduped_data.developer_name,
            deduped_data.icon_url_100,
            deduped_data.app_category,
            deduped_data.installs_d30,
            deduped_data.rating_count_d30,
            deduped_data.sdk,
            deduped_data.api_call,
            deduped_data.app_ads_direct,
            row_number() OVER (PARTITION BY deduped_data.store, deduped_data.company_domain, deduped_data.company_name ORDER BY (COALESCE((deduped_data.sdk)::integer, 0) + COALESCE((deduped_data.api_call)::integer, 0)) DESC, GREATEST((COALESCE(deduped_data.rating_count_d30, (0)::numeric))::double precision, COALESCE((deduped_data.installs_d30)::double precision, (0)::double precision)) DESC) AS app_company_rank,
            row_number() OVER (PARTITION BY deduped_data.store, deduped_data.app_category, deduped_data.company_domain, deduped_data.company_name ORDER BY (COALESCE((deduped_data.sdk)::integer, 0) + COALESCE((deduped_data.api_call)::integer, 0)) DESC, GREATEST((COALESCE(deduped_data.rating_count_d30, (0)::numeric))::double precision, COALESCE((deduped_data.installs_d30)::double precision, (0)::double precision)) DESC) AS app_company_category_rank
           FROM deduped_data
        )
 SELECT company_domain,
    company_name,
    store,
    name,
    store_id,
    developer_name,
    icon_url_100,
    app_category,
    installs_d30,
    rating_count_d30,
    sdk,
    api_call,
    app_ads_direct,
    app_company_rank,
    app_company_category_rank
   FROM ranked_apps
  WHERE (app_company_category_rank <= 20)
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

CREATE UNIQUE INDEX idx_unique_company_top_apps ON frontend.company_top_apps USING btree (company_domain, company_name, store, name, store_id, app_category);


--
-- PostgreSQL database dump complete
--

\unrestrict Fqnipgpngj6nmf3MpEwqMl2j2tcKnRTG2STP003feAKGm1uNvkQItscbrVi1KtZ

