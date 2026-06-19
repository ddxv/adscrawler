--
-- PostgreSQL database dump
--

\restrict eUfOLVoB4LH3lfcpFleKjcH8oFmcqNLZW4PNZpsYEfBJ9Zp9ga6nZ6Vkc4SQvDY

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
-- Name: companies_secondary_domain_category_tag_stats; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.companies_secondary_domain_category_tag_stats AS
 WITH distinct_apps_group AS (
         SELECT csac.store_app,
            tag.tag_source,
            d.domain_name
           FROM (((adtech.combined_app_companies csac
             LEFT JOIN adtech.companies c ON ((csac.company_id = c.id)))
             LEFT JOIN public.domains d ON ((csac.domain_id = d.id)))
             CROSS JOIN LATERAL ( VALUES ('sdk'::text,csac.sdk), ('api_call'::text,csac.api_call), ('publisher'::text,csac.publisher), ('app_ads_direct'::text,csac.app_ads_direct), ('app_ads_reseller'::text,csac.app_ads_reseller)) tag(tag_source, present))
          WHERE (csac.domain_id <> c.domain_id)
        )
 SELECT sa.store,
    sa.category AS app_category,
    dag.domain_name,
    dag.tag_source,
    count(DISTINCT dag.store_app) AS app_count,
    sum(sa.installs_sum_4w) AS installs_d30,
    sum(sa.installs) AS installs_total
   FROM (distinct_apps_group dag
     LEFT JOIN frontend.store_apps_overview sa ON ((dag.store_app = sa.id)))
  GROUP BY sa.store, sa.category, dag.domain_name, dag.tag_source
  WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.companies_secondary_domain_category_tag_stats OWNER TO postgres;

--
-- Name: companies_secondary_category_tag_stats_idx; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE UNIQUE INDEX companies_secondary_category_tag_stats_idx ON frontend.companies_secondary_domain_category_tag_stats USING btree (store, domain_name, app_category, tag_source);


--
-- Name: companies_secondary_category_tag_stats_query_idx; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE INDEX companies_secondary_category_tag_stats_query_idx ON frontend.companies_secondary_domain_category_tag_stats USING btree (domain_name);


--
-- PostgreSQL database dump complete
--

\unrestrict eUfOLVoB4LH3lfcpFleKjcH8oFmcqNLZW4PNZpsYEfBJ9Zp9ga6nZ6Vkc4SQvDY

