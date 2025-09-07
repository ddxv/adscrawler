--
-- PostgreSQL database dump
--

-- Dumped from database version 17.5 (Ubuntu 17.5-1.pgdg24.04+1)
-- Dumped by pg_dump version 17.5 (Ubuntu 17.5-1.pgdg24.04+1)

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
-- Name: store_app_z_scores; Type: MATERIALIZED VIEW; Schema: public; Owner: postgres
--

CREATE MATERIALIZED VIEW public.store_app_z_scores AS
 WITH latest_week AS (
         SELECT max(store_apps_history_weekly.week_start) AS max_week
           FROM public.store_apps_history_weekly
        ), latest_week_per_app AS (
         SELECT store_apps_history_weekly.store_app,
            max(store_apps_history_weekly.week_start) AS app_max_week
           FROM public.store_apps_history_weekly
          WHERE (store_apps_history_weekly.country_id = 840)
          GROUP BY store_apps_history_weekly.store_app
        ), baseline_period AS (
         SELECT store_apps_history_weekly.store_app,
            avg(store_apps_history_weekly.installs_diff) AS avg_installs_diff,
            stddev(store_apps_history_weekly.installs_diff) AS stddev_installs_diff,
            avg(store_apps_history_weekly.rating_count_diff) AS avg_rating_diff,
            stddev(store_apps_history_weekly.rating_count_diff) AS stddev_rating_diff
           FROM public.store_apps_history_weekly,
            latest_week
          WHERE (((store_apps_history_weekly.week_start >= (latest_week.max_week - '84 days'::interval)) AND (store_apps_history_weekly.week_start <= (latest_week.max_week - '35 days'::interval))) AND (store_apps_history_weekly.country_id = 840))
          GROUP BY store_apps_history_weekly.store_app
        ), recent_data AS (
         SELECT s.store_app,
            lw.max_week,
            s.week_start,
            s.installs_diff,
            s.rating_count_diff,
                CASE
                    WHEN (s.week_start = lwpa.app_max_week) THEN 1
                    ELSE 0
                END AS is_latest_week,
                CASE
                    WHEN (s.week_start >= (lwpa.app_max_week - '14 days'::interval)) THEN 1
                    ELSE 0
                END AS in_2w_period,
                CASE
                    WHEN (s.week_start >= (lwpa.app_max_week - '28 days'::interval)) THEN 1
                    ELSE 0
                END AS in_4w_period
           FROM ((public.store_apps_history_weekly s
             CROSS JOIN latest_week lw)
             JOIN latest_week_per_app lwpa ON ((s.store_app = lwpa.store_app)))
          WHERE ((s.week_start >= (lw.max_week - '28 days'::interval)) AND (s.country_id = 840))
        ), aggregated_metrics AS (
         SELECT rd.store_app,
            rd.max_week AS latest_week,
            sum(
                CASE
                    WHEN (rd.is_latest_week = 1) THEN rd.installs_diff
                    ELSE (0)::numeric
                END) AS installs_sum_1w,
            sum(
                CASE
                    WHEN (rd.is_latest_week = 1) THEN rd.rating_count_diff
                    ELSE (0)::bigint
                END) AS ratings_sum_1w,
            (sum(
                CASE
                    WHEN (rd.in_2w_period = 1) THEN rd.installs_diff
                    ELSE (0)::numeric
                END) / (NULLIF(sum(
                CASE
                    WHEN (rd.in_2w_period = 1) THEN 1
                    ELSE 0
                END), 0))::numeric) AS installs_avg_2w,
            (sum(
                CASE
                    WHEN (rd.in_2w_period = 1) THEN rd.rating_count_diff
                    ELSE (0)::bigint
                END) / (NULLIF(sum(
                CASE
                    WHEN (rd.in_2w_period = 1) THEN 1
                    ELSE 0
                END), 0))::numeric) AS ratings_avg_2w,
            sum(
                CASE
                    WHEN (rd.in_4w_period = 1) THEN rd.installs_diff
                    ELSE (0)::numeric
                END) AS installs_sum_4w,
            sum(
                CASE
                    WHEN (rd.in_4w_period = 1) THEN rd.rating_count_diff
                    ELSE (0)::bigint
                END) AS ratings_sum_4w,
            (sum(
                CASE
                    WHEN (rd.in_4w_period = 1) THEN rd.installs_diff
                    ELSE (0)::numeric
                END) / (NULLIF(sum(
                CASE
                    WHEN (rd.in_4w_period = 1) THEN 1
                    ELSE 0
                END), 0))::numeric) AS installs_avg_4w,
            (sum(
                CASE
                    WHEN (rd.in_4w_period = 1) THEN rd.rating_count_diff
                    ELSE (0)::bigint
                END) / (NULLIF(sum(
                CASE
                    WHEN (rd.in_4w_period = 1) THEN 1
                    ELSE 0
                END), 0))::numeric) AS ratings_avg_4w
           FROM recent_data rd
          GROUP BY rd.store_app, rd.max_week
        )
 SELECT am.store_app,
    am.latest_week,
    am.installs_sum_1w,
    am.ratings_sum_1w,
    am.installs_avg_2w,
    am.ratings_avg_2w,
    ((am.installs_avg_2w - bp.avg_installs_diff) / NULLIF(bp.stddev_installs_diff, (0)::numeric)) AS installs_z_score_2w,
    ((am.ratings_avg_2w - bp.avg_rating_diff) / NULLIF(bp.stddev_rating_diff, (0)::numeric)) AS ratings_z_score_2w,
    am.installs_sum_4w,
    am.ratings_sum_4w,
    am.installs_avg_4w,
    am.ratings_avg_4w,
    ((am.installs_avg_4w - bp.avg_installs_diff) / NULLIF(bp.stddev_installs_diff, (0)::numeric)) AS installs_z_score_4w,
    ((am.ratings_avg_4w - bp.avg_rating_diff) / NULLIF(bp.stddev_rating_diff, (0)::numeric)) AS ratings_z_score_4w
   FROM (aggregated_metrics am
     JOIN baseline_period bp ON ((am.store_app = bp.store_app)))
  WITH NO DATA;


ALTER MATERIALIZED VIEW public.store_app_z_scores OWNER TO postgres;

--
-- PostgreSQL database dump complete
--

