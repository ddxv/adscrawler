--
-- PostgreSQL database dump
--

\restrict N0kECXaWcFxdoQuhifTmoP4trdIAECUeR0ExoFRDGpXcRP6ryaQBNXCQulLHnMB

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
-- Name: app_global_metrics_weekly_diffs; Type: MATERIALIZED VIEW; Schema: public; Owner: postgres
--

CREATE MATERIALIZED VIEW public.app_global_metrics_weekly_diffs AS
 WITH snapshot_diffs AS (
         SELECT sach.store_app,
            sach.snapshot_date,
            (date_trunc('week'::text, (sach.snapshot_date)::timestamp with time zone))::date AS week_start,
            (sach.installs - lag(sach.installs) OVER (PARTITION BY sach.store_app ORDER BY sach.snapshot_date)) AS installs_diff,
            (sach.rating_count - lag(sach.rating_count) OVER (PARTITION BY sach.store_app ORDER BY sach.snapshot_date)) AS rating_count_diff
           FROM public.app_global_metrics_history sach
          WHERE ((sach.store_app IN ( SELECT store_apps.id
                   FROM public.store_apps
                  WHERE (store_apps.crawl_result = 1))) AND (sach.snapshot_date > (CURRENT_DATE - '375 days'::interval)))
        )
 SELECT week_start,
    store_app,
    COALESCE(sum(installs_diff), (0)::numeric) AS installs_diff,
    COALESCE(sum(rating_count_diff), (0)::numeric) AS rating_count_diff
   FROM snapshot_diffs
  GROUP BY week_start, store_app
  ORDER BY week_start DESC, store_app
  WITH NO DATA;


ALTER MATERIALIZED VIEW public.app_global_metrics_weekly_diffs OWNER TO postgres;

--
-- Name: app_global_metrics_weekly_diffs_week_start_store_app_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE UNIQUE INDEX app_global_metrics_weekly_diffs_week_start_store_app_idx ON public.app_global_metrics_weekly_diffs USING btree (week_start, store_app);


--
-- PostgreSQL database dump complete
--

\unrestrict N0kECXaWcFxdoQuhifTmoP4trdIAECUeR0ExoFRDGpXcRP6ryaQBNXCQulLHnMB

