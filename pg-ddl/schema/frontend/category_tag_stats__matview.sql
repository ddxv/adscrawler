--
-- PostgreSQL database dump
--

\restrict R3NXdgooEwajfBbtcShMvSzogHqxYAs8jFx9Ezyx562yjZVHyVsC3vGUGLkKuNf

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
-- Name: category_tag_stats; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.category_tag_stats AS
 WITH distinct_apps_group AS (
         SELECT DISTINCT csac.store_app,
            tag.tag_source
           FROM (adtech.combined_store_apps_companies csac
             CROSS JOIN LATERAL ( VALUES ('sdk'::text,csac.sdk), ('api_call'::text,csac.api_call), ('app_ads_direct'::text,csac.app_ads_direct), ('app_ads_reseller'::text,csac.app_ads_reseller)) tag(tag_source, present))
          WHERE (tag.present IS TRUE)
        )
 SELECT sa.store,
    sa.category AS app_category,
    dag.tag_source,
    count(DISTINCT dag.store_app) AS app_count,
    sum(sa.installs_sum_4w_est) AS installs_d30,
    sum(sa.installs_est) AS installs_total
   FROM (distinct_apps_group dag
     LEFT JOIN frontend.store_apps_overview sa ON ((dag.store_app = sa.id)))
  GROUP BY sa.store, sa.category, dag.tag_source
  WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.category_tag_stats OWNER TO postgres;

--
-- Name: idx_category_tag_stats; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE UNIQUE INDEX idx_category_tag_stats ON frontend.category_tag_stats USING btree (store, app_category, tag_source);


--
-- PostgreSQL database dump complete
--

\unrestrict R3NXdgooEwajfBbtcShMvSzogHqxYAs8jFx9Ezyx562yjZVHyVsC3vGUGLkKuNf

