--
-- PostgreSQL database dump
--

\restrict 1iUWx0zcMMzcPKUK0wEXjlglXenyEYtNWqU2A4UmmrzU8aGUdpS4sKNR5rl9Srb

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
-- Name: z_scores_top_apps; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.z_scores_top_apps AS
 WITH app_metrics AS (
         SELECT app_global_metrics_latest.store_app,
            app_global_metrics_latest.rating,
            app_global_metrics_latest.rating_count,
            app_global_metrics_latest.installs
           FROM public.app_global_metrics_latest
        ), ranked_z_scores AS (
         SELECT saz.store_app,
            saz.latest_week,
            saz.installs_sum_1w,
            saz.ratings_sum_1w,
            saz.installs_avg_2w,
            saz.ratings_avg_2w,
            saz.installs_z_score_2w,
            saz.ratings_z_score_2w,
            sa.installs_sum_4w_est AS installs_sum_4w,
            saz.ratings_sum_4w,
            saz.installs_avg_4w,
            saz.ratings_avg_4w,
            saz.installs_z_score_4w,
            saz.ratings_z_score_4w,
            sa.id,
            sa.developer_id,
            sa.developer_name,
            sa.name,
            sa.store_id,
            sa.store,
            sa.category AS app_category,
            sa.installs_est AS installs,
            sa.free,
            sa.store_last_updated,
            sa.ad_supported,
            sa.in_app_purchases,
            sa.created_at,
            sa.updated_at,
            sa.crawl_result,
            sa.release_date,
            am.rating_count,
            sa.icon_url_100,
            row_number() OVER (PARTITION BY sa.store, sa.category,
                CASE
                    WHEN (sa.store = 2) THEN 'rating'::text
                    ELSE 'installs'::text
                END ORDER BY
                CASE
                    WHEN (sa.store = 2) THEN saz.ratings_z_score_2w
                    WHEN (sa.store = 1) THEN saz.installs_z_score_2w
                    ELSE NULL::numeric
                END DESC NULLS LAST) AS rn
           FROM ((public.store_app_z_scores saz
             LEFT JOIN frontend.store_apps_overview sa ON ((saz.store_app = sa.id)))
             LEFT JOIN app_metrics am ON ((saz.store_app = am.store_app)))
          WHERE (sa.store = ANY (ARRAY[1, 2]))
        )
 SELECT store,
    store_id,
    name AS app_name,
    developer_name,
    app_category,
    in_app_purchases,
    ad_supported,
    icon_url_100,
    installs,
    rating_count,
    installs_sum_1w,
    ratings_sum_1w,
    installs_avg_2w,
    ratings_avg_2w,
    installs_z_score_2w,
    ratings_z_score_2w,
    installs_sum_4w,
    ratings_sum_4w,
    installs_avg_4w,
    ratings_avg_4w,
    installs_z_score_4w,
    ratings_z_score_4w
   FROM ranked_z_scores
  WHERE (rn <= 100)
  WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.z_scores_top_apps OWNER TO postgres;

--
-- Name: frontend_z_scores_top_apps_unique; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE UNIQUE INDEX frontend_z_scores_top_apps_unique ON frontend.z_scores_top_apps USING btree (store, store_id);


--
-- PostgreSQL database dump complete
--

\unrestrict 1iUWx0zcMMzcPKUK0wEXjlglXenyEYtNWqU2A4UmmrzU8aGUdpS4sKNR5rl9Srb

