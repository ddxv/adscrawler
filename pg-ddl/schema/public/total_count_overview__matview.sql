--
-- PostgreSQL database dump
--

\restrict TXYhbh0ESylfMxvpcxdahUevcY3XL79MprHzOFMoaUvmWaUdJqw3tGKJwzk37q4

-- Dumped from database version 18.4 (Ubuntu 18.4-1.pgdg26.04+1)
-- Dumped by pg_dump version 18.4 (Ubuntu 18.4-1.pgdg26.04+1)

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
                    WHEN ((sa.store = 1) AND (NOT sa.is_removed)) THEN 1
                    ELSE NULL::integer
                END) AS success_android_apps,
            count(
                CASE
                    WHEN ((sa.store = 2) AND (NOT sa.is_removed)) THEN 1
                    ELSE NULL::integer
                END) AS success_ios_apps,
            count(
                CASE
                    WHEN ((sa.store = 1) AND (sa.last_crawled_at >= (CURRENT_DATE - '7 days'::interval))) THEN 1
                    ELSE NULL::integer
                END) AS weekly_scanned_android_apps,
            count(
                CASE
                    WHEN ((sa.store = 2) AND (sa.last_crawled_at >= (CURRENT_DATE - '7 days'::interval))) THEN 1
                    ELSE NULL::integer
                END) AS weekly_scanned_ios_apps,
            count(
                CASE
                    WHEN ((sa.store = 1) AND (NOT sa.is_removed) AND (sa.last_crawled_at >= (CURRENT_DATE - '7 days'::interval))) THEN 1
                    ELSE NULL::integer
                END) AS weekly_success_scanned_android_apps,
            count(
                CASE
                    WHEN ((sa.store = 2) AND (NOT sa.is_removed) AND (sa.last_crawled_at >= (CURRENT_DATE - '7 days'::interval))) THEN 1
                    ELSE NULL::integer
                END) AS weekly_success_scanned_ios_apps
           FROM frontend.store_apps_overview sa
        ), last_scan AS (
         SELECT DISTINCT ON (vc.store_app) vc.store_app,
            lsscr.version_code_id AS version_code,
            lsscr.scanned_at,
            lsscr.scan_result
           FROM (public.version_code_sdk_scan_results lsscr
             LEFT JOIN public.version_codes vc ON ((lsscr.version_code_id = vc.id)))
          ORDER BY vc.store_app, lsscr.scanned_at DESC
        ), last_scan_succeed AS (
         SELECT DISTINCT ON (vc.store_app) vc.id,
            vc.store_app,
            vc.version_code,
            vcss.scanned_at,
            vcss.scan_result
           FROM (public.version_codes vc
             LEFT JOIN public.version_code_sdk_scan_results vcss ON ((vc.id = vcss.version_code_id)))
          WHERE (vcss.scan_result = 1)
          ORDER BY vc.store_app, vcss.scanned_at DESC, (string_to_array((vc.version_code)::text, '.'::text))::bigint[] DESC
        ), sdk_app_count AS (
         SELECT count(DISTINCT
                CASE
                    WHEN (sa.store = 1) THEN ls.store_app
                    ELSE NULL::integer
                END) AS sdk_android_apps,
            count(DISTINCT
                CASE
                    WHEN (sa.store = 2) THEN ls.store_app
                    ELSE NULL::integer
                END) AS sdk_ios_apps,
            count(DISTINCT
                CASE
                    WHEN (sa.store = 1) THEN lss.store_app
                    ELSE NULL::integer
                END) AS sdk_success_android_apps,
            count(DISTINCT
                CASE
                    WHEN (sa.store = 2) THEN lss.store_app
                    ELSE NULL::integer
                END) AS sdk_success_ios_apps,
            count(DISTINCT
                CASE
                    WHEN ((sa.store = 1) AND (lss.scanned_at >= (CURRENT_DATE - '7 days'::interval))) THEN lss.store_app
                    ELSE NULL::integer
                END) AS sdk_weekly_success_android_apps,
            count(DISTINCT
                CASE
                    WHEN ((sa.store = 2) AND (lss.scanned_at >= (CURRENT_DATE - '7 days'::interval))) THEN lss.store_app
                    ELSE NULL::integer
                END) AS sdk_weekly_success_ios_apps,
            count(DISTINCT
                CASE
                    WHEN ((sa.store = 1) AND (ls.scanned_at >= (CURRENT_DATE - '7 days'::interval))) THEN ls.store_app
                    ELSE NULL::integer
                END) AS sdk_weekly_android_apps,
            count(DISTINCT
                CASE
                    WHEN ((sa.store = 2) AND (ls.scanned_at >= (CURRENT_DATE - '7 days'::interval))) THEN ls.store_app
                    ELSE NULL::integer
                END) AS sdk_weekly_ios_apps
           FROM ((last_scan ls
             LEFT JOIN last_scan_succeed lss ON ((ls.store_app = lss.store_app)))
             LEFT JOIN frontend.store_apps_overview sa ON ((sa.id = ls.store_app)))
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

\unrestrict TXYhbh0ESylfMxvpcxdahUevcY3XL79MprHzOFMoaUvmWaUdJqw3tGKJwzk37q4

