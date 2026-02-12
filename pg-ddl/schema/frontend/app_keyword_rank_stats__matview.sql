--
-- PostgreSQL database dump
--

\restrict mQkiW29NTruEqA9NxaxOsWGaGNhYL4Hyn6tZylWPcEl8dH2tHZGVJZFIm5w4y0a

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
-- Name: app_keyword_rank_stats; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.app_keyword_rank_stats AS
 WITH latest_per_country AS (
         SELECT app_keyword_ranks_daily.country,
            max(app_keyword_ranks_daily.crawled_date) AS max_crawled_date
           FROM frontend.app_keyword_ranks_daily
          GROUP BY app_keyword_ranks_daily.country
        ), d30_keywords AS (
         SELECT akr.country,
            akr.store_app,
            akr.keyword_id,
            min(akr.app_rank) AS d30_best_rank
           FROM frontend.app_keyword_ranks_daily akr
          WHERE (akr.crawled_date >= (CURRENT_DATE - '30 days'::interval))
          GROUP BY akr.country, akr.store_app, akr.keyword_id
        ), latest_ranks AS (
         SELECT kr.country,
            kr.store_app,
            kr.keyword_id,
            kr.app_rank AS latest_app_rank
           FROM (frontend.app_keyword_ranks_daily kr
             JOIN latest_per_country lpc ON (((kr.country = lpc.country) AND (kr.crawled_date = lpc.max_crawled_date))))
        ), all_ranked_keywords AS (
         SELECT rk.country,
            rk.store_app,
            rk.keyword_id,
            rk.d30_best_rank,
            lk.latest_app_rank
           FROM (d30_keywords rk
             LEFT JOIN latest_ranks lk ON (((lk.country = rk.country) AND (lk.store_app = rk.store_app) AND (lk.keyword_id = rk.keyword_id))))
        )
 SELECT country,
    store_app,
    keyword_id,
    d30_best_rank,
    latest_app_rank
   FROM all_ranked_keywords
  WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.app_keyword_rank_stats OWNER TO postgres;

--
-- PostgreSQL database dump complete
--

\unrestrict mQkiW29NTruEqA9NxaxOsWGaGNhYL4Hyn6tZylWPcEl8dH2tHZGVJZFIm5w4y0a

