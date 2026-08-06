--
-- PostgreSQL database dump
--

\restrict bjeWGg4WqwyWDitImG2sTedabOMp7NSYNHZtAWwDAFvyHImTuZVYHSt3PS3KNFU

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
-- Name: latest_sdk_scanned_apps; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.latest_sdk_scanned_apps AS
 WITH last_successful_scanned AS (
         SELECT DISTINCT ON (vc.store_app) vc.store_app,
            vasr.version_code_id,
            vc.version_code,
            vasr.run_result AS crawl_result,
            vasr.run_at
           FROM (public.version_code_api_scan_results vasr
             LEFT JOIN public.version_codes vc ON ((vasr.version_code_id = vc.id)))
          ORDER BY vc.store_app, vasr.run_at DESC
        ), ranked_apps AS (
         SELECT lss.run_at AS sdk_crawled_at,
            lss.version_code,
            lss.crawl_result,
            sa.store,
            sa.store_id,
            sa.name,
            sa.installs,
            sa.rating_count,
            row_number() OVER (PARTITION BY sa.store, lss.crawl_result ORDER BY lss.run_at DESC) AS updated_rank
           FROM (last_successful_scanned lss
             LEFT JOIN frontend.store_apps_overview sa ON ((lss.store_app = sa.id)))
          WHERE (lss.run_at <= (CURRENT_DATE - '1 day'::interval))
        )
 SELECT sdk_crawled_at,
    version_code,
    crawl_result,
    store,
    store_id,
    name,
    installs,
    rating_count,
    updated_rank
   FROM ranked_apps
  WHERE (updated_rank <= 100)
  WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.latest_sdk_scanned_apps OWNER TO postgres;

--
-- Name: latest_sdk_scanned_apps_unique_index; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE UNIQUE INDEX latest_sdk_scanned_apps_unique_index ON frontend.latest_sdk_scanned_apps USING btree (version_code, crawl_result, store, store_id);


--
-- PostgreSQL database dump complete
--

\unrestrict bjeWGg4WqwyWDitImG2sTedabOMp7NSYNHZtAWwDAFvyHImTuZVYHSt3PS3KNFU

