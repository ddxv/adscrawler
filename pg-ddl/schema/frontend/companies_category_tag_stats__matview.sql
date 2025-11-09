--
-- PostgreSQL database dump
--

\restrict 9Ot6J3fpm1qvlgOkMcxu1UbBcubcBIqCgkNMb3c1E0yghC6fMdoiahARJApdtYy

-- Dumped from database version 18.0 (Ubuntu 18.0-1.pgdg24.04+3)
-- Dumped by pg_dump version 18.0 (Ubuntu 18.0-1.pgdg24.04+3)

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
 WITH d30_counts AS (
         SELECT sahw.store_app,
            sum(sahw.installs_diff) AS d30_installs,
            sum(sahw.rating_count_diff) AS d30_rating_count
           FROM public.app_global_metrics_weekly_diffs sahw
          WHERE ((sahw.week_start > (CURRENT_DATE - '31 days'::interval)) AND ((sahw.installs_diff > (0)::numeric) OR (sahw.rating_count_diff > (0)::numeric)))
          GROUP BY sahw.store_app
        ), distinct_apps_group AS (
         SELECT sa.store,
            csac.store_app,
            csac.app_category,
            tag.tag_source,
            csac.ad_domain AS company_domain,
            c.name AS company_name,
            sa.installs,
            sa.rating_count
           FROM (((adtech.combined_store_apps_companies csac
             LEFT JOIN adtech.companies c ON ((csac.company_id = c.id)))
             LEFT JOIN frontend.store_apps_overview sa ON ((csac.store_app = sa.id)))
             CROSS JOIN LATERAL ( VALUES ('sdk'::text,csac.sdk), ('api_call'::text,csac.api_call), ('app_ads_direct'::text,csac.app_ads_direct), ('app_ads_reseller'::text,csac.app_ads_reseller)) tag(tag_source, present))
          WHERE (tag.present IS TRUE)
        )
 SELECT dag.store,
    dag.app_category,
    dag.tag_source,
    dag.company_domain,
    dag.company_name,
    count(DISTINCT dag.store_app) AS app_count,
    sum(dc.d30_installs) AS installs_d30,
    sum(dc.d30_rating_count) AS rating_count_d30,
    sum(dag.installs) AS installs_total,
    sum(dag.rating_count) AS rating_count_total
   FROM (distinct_apps_group dag
     LEFT JOIN d30_counts dc ON ((dag.store_app = dc.store_app)))
  GROUP BY dag.store, dag.app_category, dag.tag_source, dag.company_domain, dag.company_name
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

\unrestrict 9Ot6J3fpm1qvlgOkMcxu1UbBcubcBIqCgkNMb3c1E0yghC6fMdoiahARJApdtYy

