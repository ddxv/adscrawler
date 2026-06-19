--
-- PostgreSQL database dump
--

\restrict daztun4i9hmy62YvQ1oqh0FUWwOoP14QdmI5SVvrNZrJacv3LpNwencTlZeWydC

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
 WITH ranked_apps AS (
         SELECT d.domain_name AS company_domain,
            c.name AS company_name,
            sa.store,
            sa.name,
            sa.store_id,
            sa.category AS app_category,
            sa.installs_sum_4w AS installs_d30,
            sa.icon_url_100,
            cac.sdk,
            cac.api_call,
            cac.publisher,
            cac.app_ads_direct,
            row_number() OVER (PARTITION BY sa.store, d.domain_name ORDER BY COALESCE((sa.installs_sum_4w)::double precision, (0)::double precision) DESC) AS app_company_rank,
            row_number() OVER (PARTITION BY sa.store, d.domain_name, sa.category ORDER BY COALESCE((sa.installs_sum_4w)::double precision, (0)::double precision) DESC) AS app_company_category_rank
           FROM ((((adtech.combined_app_companies cac
             LEFT JOIN public.domains d ON ((cac.domain_id = d.id)))
             LEFT JOIN adtech.company_domain_mapping cdm ON ((cac.domain_id = cdm.domain_id)))
             LEFT JOIN adtech.companies c ON ((cdm.company_id = c.id)))
             LEFT JOIN frontend.store_apps_overview sa ON ((cac.store_app = sa.id)))
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
    publisher,
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

\unrestrict daztun4i9hmy62YvQ1oqh0FUWwOoP14QdmI5SVvrNZrJacv3LpNwencTlZeWydC

