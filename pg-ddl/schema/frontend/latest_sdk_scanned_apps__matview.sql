--
-- PostgreSQL database dump
--

\restrict yxbuFX85tn01LhSvzTOZpz1mEGlGcgIMmZU5rGz2skzxnF8Y6UjJbYzx1zAlcFK

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
-- Name: latest_sdk_scanned_apps; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.latest_sdk_scanned_apps AS
 WITH latest_version_codes AS (
         SELECT DISTINCT ON (version_codes.store_app) version_codes.id,
            version_codes.store_app,
            version_codes.version_code,
            version_codes.updated_at,
            version_codes.crawl_result
           FROM public.version_codes
          ORDER BY version_codes.store_app, (string_to_array((version_codes.version_code)::text, '.'::text))::bigint[] DESC
        ), ranked_apps AS (
         SELECT lvc.updated_at AS sdk_crawled_at,
            lvc.version_code,
            lvc.crawl_result,
            sa.store,
            sa.store_id,
            sa.name,
            sa.installs,
            sa.rating_count,
            row_number() OVER (PARTITION BY sa.store, lvc.crawl_result ORDER BY lvc.updated_at DESC) AS updated_rank
           FROM (latest_version_codes lvc
             LEFT JOIN frontend.store_apps_overview sa ON ((lvc.store_app = sa.id)))
          WHERE (lvc.updated_at <= (CURRENT_DATE - '1 day'::interval))
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

\unrestrict yxbuFX85tn01LhSvzTOZpz1mEGlGcgIMmZU5rGz2skzxnF8Y6UjJbYzx1zAlcFK

