--
-- PostgreSQL database dump
--

\restrict CYElgV2E3gN9AGY07Y8kDEvsYhV85bmkeMNRfm6XWq3PRCWVQRjoKZKTWNrKEVC

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
-- Name: companies_category_tag_stats; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.companies_category_tag_stats AS
 WITH distinct_apps_group AS (
         SELECT csac.store_app,
            csac.app_category,
            tag.tag_source,
            csac.ad_domain AS company_domain,
            c.name AS company_name
           FROM ((adtech.combined_store_apps_companies csac
             LEFT JOIN adtech.companies c ON ((csac.company_id = c.id)))
             CROSS JOIN LATERAL ( VALUES ('sdk'::text,csac.sdk), ('api_call'::text,csac.api_call), ('app_ads_direct'::text,csac.app_ads_direct), ('app_ads_reseller'::text,csac.app_ads_reseller)) tag(tag_source, present))
          WHERE (tag.present IS TRUE)
        )
 SELECT sa.store,
    sa.category AS app_category,
    dag.tag_source,
    dag.company_domain,
    dag.company_name,
    count(DISTINCT dag.store_app) AS app_count,
    sum(sa.installs_sum_4w_est) AS installs_d30,
    sum(sa.installs_est) AS installs_total
   FROM (distinct_apps_group dag
     LEFT JOIN frontend.store_apps_overview sa ON ((dag.store_app = sa.id)))
  GROUP BY sa.store, sa.category, dag.tag_source, dag.company_domain, dag.company_name
  WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.companies_category_tag_stats OWNER TO postgres;

--
-- Name: companies_category_tag_stats__query_idx; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE INDEX companies_category_tag_stats__query_idx ON frontend.companies_category_tag_stats USING btree (company_domain);


--
-- Name: companies_category_tag_stats_idx; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE UNIQUE INDEX companies_category_tag_stats_idx ON frontend.companies_category_tag_stats USING btree (store, tag_source, app_category, company_domain);


--
-- PostgreSQL database dump complete
--

\unrestrict CYElgV2E3gN9AGY07Y8kDEvsYhV85bmkeMNRfm6XWq3PRCWVQRjoKZKTWNrKEVC

