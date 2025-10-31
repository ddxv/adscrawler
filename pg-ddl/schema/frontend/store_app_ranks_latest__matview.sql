--
-- PostgreSQL database dump
--

\restrict 5BVBFrMCMr0cXN1WWF4Ub5PdB5hxYf9icqjsrvsX9yuPyIc9odm6fQdPdAd5ncl

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
-- Name: store_app_ranks_latest; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.store_app_ranks_latest AS
 WITH latest_crawled_date AS (
         SELECT arr.store_collection,
            arr.country,
            max(arr.crawled_date) AS crawled_date
           FROM frontend.store_app_ranks_weekly arr
          GROUP BY arr.store_collection, arr.country
        )
 SELECT ar.rank,
    sa.name,
    sa.store_id,
    sa.store,
    sa.installs,
    sa.rating_count,
    sa.rating,
    sa.installs_sum_1w,
    sa.installs_sum_4w,
    sa.ratings_sum_1w,
    sa.ratings_sum_4w,
    sa.icon_url_100,
    ar.store_collection,
    ar.store_category,
    c.alpha2 AS country,
    ar.crawled_date
   FROM (((frontend.store_app_ranks_weekly ar
     JOIN latest_crawled_date lcd ON (((ar.store_collection = lcd.store_collection) AND (ar.country = lcd.country) AND (ar.crawled_date = lcd.crawled_date))))
     JOIN public.countries c ON ((ar.country = c.id)))
     LEFT JOIN frontend.store_apps_overview sa ON ((ar.store_app = sa.id)))
  ORDER BY ar.rank
  WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.store_app_ranks_latest OWNER TO postgres;

--
-- Name: idx_store_app_ranks_latest_filter_sort; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE UNIQUE INDEX idx_store_app_ranks_latest_filter_sort ON frontend.store_app_ranks_latest USING btree (store_collection, store_category, country, rank);


--
-- PostgreSQL database dump complete
--

\unrestrict 5BVBFrMCMr0cXN1WWF4Ub5PdB5hxYf9icqjsrvsX9yuPyIc9odm6fQdPdAd5ncl

