--
-- PostgreSQL database dump
--

\restrict v0RGuutv2lyeKFhaeijXcexDpBv5ajhK0fdJSQSX3khpmA3gBlk4HgoTFaCUQay

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
-- Name: companies_parent_category_stats; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.companies_parent_category_stats AS
 WITH distinct_apps_group AS (
         SELECT DISTINCT csac.store_app,
            COALESCE(ad.domain_name, csac.ad_domain) AS company_domain,
            c.name AS company_name
           FROM ((adtech.combined_store_apps_companies csac
             LEFT JOIN adtech.companies c ON ((csac.parent_id = c.id)))
             LEFT JOIN public.domains ad ON ((c.domain_id = ad.id)))
          WHERE (csac.parent_id IN ( SELECT DISTINCT pc.id
                   FROM (adtech.companies pc
                     LEFT JOIN adtech.companies c_1 ON ((pc.id = c_1.parent_company_id)))
                  WHERE (c_1.id IS NOT NULL)))
        )
 SELECT sa.store,
    sa.category AS app_category,
    dag.company_domain,
    dag.company_name,
    count(DISTINCT dag.store_app) AS app_count,
    sum(sa.installs_sum_4w_est) AS installs_d30,
    sum(sa.installs_est) AS installs_total
   FROM (distinct_apps_group dag
     LEFT JOIN frontend.store_apps_overview sa ON ((dag.store_app = sa.id)))
  GROUP BY sa.store, sa.category, dag.company_domain, dag.company_name
  WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.companies_parent_category_stats OWNER TO postgres;

--
-- Name: companies_parent_category_stats_idx; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE UNIQUE INDEX companies_parent_category_stats_idx ON frontend.companies_parent_category_stats USING btree (store, company_domain, company_name, app_category);


--
-- Name: companies_parent_category_stats_query_idx; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE INDEX companies_parent_category_stats_query_idx ON frontend.companies_parent_category_stats USING btree (company_domain);


--
-- PostgreSQL database dump complete
--

\unrestrict v0RGuutv2lyeKFhaeijXcexDpBv5ajhK0fdJSQSX3khpmA3gBlk4HgoTFaCUQay

