--
-- PostgreSQL database dump
--

\restrict YqL5W1A7Lba42AOLGEQvIyy6JeRyPMAOfcIao4MIe3qqLtK0zvH0fIaFM9vZ7e3

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
-- Name: app_global_metrics_latest; Type: MATERIALIZED VIEW; Schema: public; Owner: postgres
--

CREATE MATERIALIZED VIEW public.app_global_metrics_latest AS
 WITH global_anchor AS (
         SELECT app_global_metrics_weekly.store_app,
            app_global_metrics_weekly.week_start,
            app_global_metrics_weekly.weekly_iap_revenue,
            app_global_metrics_weekly.weekly_ad_revenue,
            app_global_metrics_weekly.weekly_installs,
            app_global_metrics_weekly.weekly_ratings,
            app_global_metrics_weekly.total_installs,
            app_global_metrics_weekly.total_ratings,
            app_global_metrics_weekly.weekly_active_users,
            app_global_metrics_weekly.monthly_active_users,
            app_global_metrics_weekly.rating,
            app_global_metrics_weekly.one_star,
            app_global_metrics_weekly.two_star,
            app_global_metrics_weekly.three_star,
            app_global_metrics_weekly.four_star,
            app_global_metrics_weekly.five_star,
            date_trunc('week'::text, (CURRENT_DATE - '2 days'::interval)) AS global_max_week
           FROM public.app_global_metrics_weekly
          WHERE (app_global_metrics_weekly.week_start >= (CURRENT_DATE - '100 days'::interval))
        ), windowed_metrics AS (
         SELECT ga.store_app,
            ga.week_start,
            ga.weekly_iap_revenue,
            ga.weekly_ad_revenue,
            ga.weekly_installs,
            ga.weekly_ratings,
            ga.total_installs,
            ga.total_ratings,
            ga.weekly_active_users,
            ga.monthly_active_users,
            ga.rating,
            ga.one_star,
            ga.two_star,
            ga.three_star,
            ga.four_star,
            ga.five_star,
            row_number() OVER w_ordered AS rn,
            sum(ga.weekly_installs) OVER w_4w AS monthly_installs,
            sum(ga.weekly_ad_revenue) OVER w_4w AS monthly_ad_revenue,
            sum(ga.weekly_iap_revenue) OVER w_4w AS monthly_iap_revenue,
            avg(ga.weekly_installs) OVER w_2w AS installs_avg_2w,
            avg(ga.weekly_installs) OVER w_4w AS installs_avg_4w,
            avg(
                CASE
                    WHEN ((ga.week_start >= (ga.global_max_week - '112 days'::interval)) AND (ga.week_start <= (ga.global_max_week - '28 days'::interval))) THEN ga.weekly_installs
                    ELSE NULL::bigint
                END) OVER w_ordered AS b_avg_installs,
            stddev(
                CASE
                    WHEN ((ga.week_start >= (ga.global_max_week - '112 days'::interval)) AND (ga.week_start <= (ga.global_max_week - '28 days'::interval))) THEN ga.weekly_installs
                    ELSE NULL::bigint
                END) OVER w_ordered AS b_std_installs
           FROM global_anchor ga
          WINDOW w_ordered AS (PARTITION BY ga.store_app ORDER BY ga.week_start DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), w_4w AS (PARTITION BY ga.store_app ORDER BY ga.week_start DESC ROWS BETWEEN CURRENT ROW AND 3 FOLLOWING), w_2w AS (PARTITION BY ga.store_app ORDER BY ga.week_start DESC ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING)
        )
 SELECT store_app,
    week_start,
    weekly_iap_revenue,
    weekly_ad_revenue,
    weekly_installs,
    weekly_ratings,
    total_installs,
    total_ratings,
    weekly_active_users,
    monthly_active_users,
    rating,
    one_star,
    two_star,
    three_star,
    four_star,
    five_star,
    monthly_iap_revenue,
    monthly_ad_revenue,
    monthly_installs,
    installs_avg_2w,
    installs_avg_4w,
    b_avg_installs,
    b_std_installs,
    ((installs_avg_2w - b_avg_installs) / NULLIF(b_std_installs, (0)::numeric)) AS installs_z_score_2w,
    ((installs_avg_4w - b_avg_installs) / NULLIF(b_std_installs, (0)::numeric)) AS installs_z_score_4w,
    ((installs_avg_2w - installs_avg_4w) / NULLIF(installs_avg_4w, (0)::numeric)) AS installs_acceleration,
    ((b_std_installs IS NOT NULL) AND (b_avg_installs > (0)::numeric)) AS has_reliable_baseline
   FROM windowed_metrics
  WHERE (rn = 1)
  WITH NO DATA;


ALTER MATERIALIZED VIEW public.app_global_metrics_latest OWNER TO postgres;

--
-- Name: app_global_metrics_latest_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE UNIQUE INDEX app_global_metrics_latest_idx ON public.app_global_metrics_latest USING btree (store_app);


--
-- PostgreSQL database dump complete
--

\unrestrict YqL5W1A7Lba42AOLGEQvIyy6JeRyPMAOfcIao4MIe3qqLtK0zvH0fIaFM9vZ7e3

