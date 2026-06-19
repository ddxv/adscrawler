--
-- PostgreSQL database dump
--

\restrict YLNfyDFK6bd4IW6sxSR3GcotlbhqhwuP3AnFZ0yyH1VXjZcts9ho4H4rFSxwK3k

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
-- Name: tag_totals; Type: MATERIALIZED VIEW; Schema: adtech; Owner: postgres
--

CREATE MATERIALIZED VIEW adtech.tag_totals AS
 WITH distinct_apps_group AS (
         SELECT DISTINCT csac.store_app,
            tag.tag_source
           FROM (adtech.combined_app_companies csac
             CROSS JOIN LATERAL ( VALUES ('sdk'::text,csac.sdk), ('api_call'::text,csac.api_call), ('publisher'::text,csac.publisher), ('app_ads_direct'::text,csac.app_ads_direct), ('app_ads_reseller'::text,csac.app_ads_reseller)) tag(tag_source, present))
          WHERE (tag.present IS TRUE)
        ), store_category_universes AS (
         SELECT store_apps_overview.store,
            store_apps_overview.category AS app_category,
            count(store_apps_overview.id) AS active_apps_universe,
            sum(store_apps_overview.installs) AS universe_installs_total,
            sum(store_apps_overview.installs_sum_4w) AS universe_installs_d30
           FROM frontend.store_apps_overview
          WHERE (store_apps_overview.id IS NOT NULL)
          GROUP BY store_apps_overview.store, store_apps_overview.category
        )
 SELECT sa.store,
    sa.category AS app_category,
    dag.tag_source,
    count(DISTINCT dag.store_app) AS total_active_scanned_apps_with_tag,
    sum(sa.installs) AS total_scanned_installs_with_tag,
    sum(sa.installs_sum_4w) AS total_scanned_installs_d30_with_tag,
    max(su.active_apps_universe) AS active_apps_universe,
    max(su.universe_installs_total) AS universe_installs_total,
    max(su.universe_installs_d30) AS universe_installs_d30
   FROM ((distinct_apps_group dag
     LEFT JOIN frontend.store_apps_overview sa ON ((dag.store_app = sa.id)))
     LEFT JOIN store_category_universes su ON (((sa.store = su.store) AND (sa.category = su.app_category))))
  WHERE (sa.id IS NOT NULL)
  GROUP BY sa.store, sa.category, dag.tag_source
  WITH NO DATA;


ALTER MATERIALIZED VIEW adtech.tag_totals OWNER TO postgres;

--
-- PostgreSQL database dump complete
--

\unrestrict YLNfyDFK6bd4IW6sxSR3GcotlbhqhwuP3AnFZ0yyH1VXjZcts9ho4H4rFSxwK3k

