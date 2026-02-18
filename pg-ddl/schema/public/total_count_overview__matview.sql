--
-- PostgreSQL database dump
--

\restrict UrTpmXYQprAreBK5VOa4YfYPDqrTY0eonPVvRvscaPOtfQvNKpoYyuIDCM8l9Ug

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
-- Name: total_count_overview; Type: MATERIALIZED VIEW; Schema: public; Owner: postgres
--

CREATE MATERIALIZED VIEW public.total_count_overview AS
 WITH app_count AS (
         SELECT count(
                CASE
                    WHEN (sa.store = 1) THEN 1
                    ELSE NULL::integer
                END) AS android_apps,
            count(
                CASE
                    WHEN (sa.store = 2) THEN 1
                    ELSE NULL::integer
                END) AS ios_apps,
            count(
                CASE
                    WHEN ((sa.store = 1) AND (sa.crawl_result = 1)) THEN 1
                    ELSE NULL::integer
                END) AS success_android_apps,
            count(
                CASE
                    WHEN ((sa.store = 2) AND (sa.crawl_result = 1)) THEN 1
                    ELSE NULL::integer
                END) AS success_ios_apps,
            count(
                CASE
                    WHEN ((sa.store = 1) AND (sa.updated_at >= (CURRENT_DATE - '7 days'::interval))) THEN 1
                    ELSE NULL::integer
                END) AS weekly_scanned_android_apps,
            count(
                CASE
                    WHEN ((sa.store = 2) AND (sa.updated_at >= (CURRENT_DATE - '7 days'::interval))) THEN 1
                    ELSE NULL::integer
                END) AS weekly_scanned_ios_apps,
            count(
                CASE
                    WHEN ((sa.store = 1) AND (sa.crawl_result = 1) AND (sa.updated_at >= (CURRENT_DATE - '7 days'::interval))) THEN 1
                    ELSE NULL::integer
                END) AS weekly_success_scanned_android_apps,
            count(
                CASE
                    WHEN ((sa.store = 2) AND (sa.crawl_result = 1) AND (sa.updated_at >= (CURRENT_DATE - '7 days'::interval))) THEN 1
                    ELSE NULL::integer
                END) AS weekly_success_scanned_ios_apps
           FROM public.store_apps sa
        ), sdk_app_count AS (
         SELECT count(DISTINCT
                CASE
                    WHEN (sa.store = 1) THEN vc.store_app
                    ELSE NULL::integer
                END) AS sdk_android_apps,
            count(DISTINCT
                CASE
                    WHEN (sa.store = 2) THEN vc.store_app
                    ELSE NULL::integer
                END) AS sdk_ios_apps,
            count(DISTINCT
                CASE
                    WHEN ((sa.store = 1) AND (vc.crawl_result = 1)) THEN vc.store_app
                    ELSE NULL::integer
                END) AS sdk_success_android_apps,
            count(DISTINCT
                CASE
                    WHEN ((sa.store = 2) AND (vc.crawl_result = 1)) THEN vc.store_app
                    ELSE NULL::integer
                END) AS sdk_success_ios_apps,
            count(DISTINCT
                CASE
                    WHEN ((sa.store = 1) AND (vc.updated_at >= (CURRENT_DATE - '7 days'::interval)) AND (vc.crawl_result = 1)) THEN vc.store_app
                    ELSE NULL::integer
                END) AS sdk_weekly_success_android_apps,
            count(DISTINCT
                CASE
                    WHEN ((sa.store = 2) AND (vc.updated_at >= (CURRENT_DATE - '7 days'::interval)) AND (vc.crawl_result = 1)) THEN vc.store_app
                    ELSE NULL::integer
                END) AS sdk_weekly_success_ios_apps,
            count(DISTINCT
                CASE
                    WHEN ((sa.store = 1) AND (vc.updated_at >= (CURRENT_DATE - '7 days'::interval))) THEN vc.store_app
                    ELSE NULL::integer
                END) AS sdk_weekly_android_apps,
            count(DISTINCT
                CASE
                    WHEN ((sa.store = 2) AND (vc.updated_at >= (CURRENT_DATE - '7 days'::interval))) THEN vc.store_app
                    ELSE NULL::integer
                END) AS sdk_weekly_ios_apps
           FROM (public.version_codes vc
             LEFT JOIN public.store_apps sa ON ((vc.store_app = sa.id)))
        ), appads_url_count AS (
         SELECT count(DISTINCT pd.domain_name) AS appads_urls,
            count(DISTINCT
                CASE
                    WHEN (pdcr.crawl_result = 1) THEN pd.domain_name
                    ELSE NULL::character varying
                END) AS appads_success_urls,
            count(DISTINCT
                CASE
                    WHEN ((pdcr.crawl_result = 1) AND (pdcr.updated_at >= (CURRENT_DATE - '7 days'::interval))) THEN pd.domain_name
                    ELSE NULL::character varying
                END) AS appads_weekly_success_urls,
            count(DISTINCT
                CASE
                    WHEN (pdcr.updated_at >= (CURRENT_DATE - '7 days'::interval)) THEN pd.domain_name
                    ELSE NULL::character varying
                END) AS appads_weekly_urls
           FROM (public.domains pd
             LEFT JOIN public.adstxt_crawl_results pdcr ON ((pd.id = pdcr.domain_id)))
        )
 SELECT app_count.android_apps,
    app_count.ios_apps,
    app_count.success_android_apps,
    app_count.success_ios_apps,
    app_count.weekly_scanned_android_apps,
    app_count.weekly_scanned_ios_apps,
    app_count.weekly_success_scanned_android_apps,
    app_count.weekly_success_scanned_ios_apps,
    sdk_app_count.sdk_android_apps,
    sdk_app_count.sdk_ios_apps,
    sdk_app_count.sdk_success_android_apps,
    sdk_app_count.sdk_success_ios_apps,
    sdk_app_count.sdk_weekly_success_android_apps,
    sdk_app_count.sdk_weekly_success_ios_apps,
    sdk_app_count.sdk_weekly_android_apps,
    sdk_app_count.sdk_weekly_ios_apps,
    appads_url_count.appads_urls,
    appads_url_count.appads_success_urls,
    appads_url_count.appads_weekly_success_urls,
    appads_url_count.appads_weekly_urls,
    CURRENT_DATE AS on_date
   FROM app_count,
    sdk_app_count,
    appads_url_count
  WITH NO DATA;


ALTER MATERIALIZED VIEW public.total_count_overview OWNER TO postgres;

--
-- PostgreSQL database dump complete
--

\unrestrict UrTpmXYQprAreBK5VOa4YfYPDqrTY0eonPVvRvscaPOtfQvNKpoYyuIDCM8l9Ug

