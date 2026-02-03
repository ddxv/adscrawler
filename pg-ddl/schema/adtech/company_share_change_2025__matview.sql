--
-- PostgreSQL database dump
--

\restrict BgXRyIjKPvCreuAubND2kXoCGnlOr1MDrT5IazYXU07U2qHj7CU38EGnX8Mpdob

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
-- Name: company_share_change_2025; Type: MATERIALIZED VIEW; Schema: adtech; Owner: postgres
--

CREATE MATERIALIZED VIEW adtech.company_share_change_2025 AS
 WITH limit_apps AS (
         SELECT DISTINCT store_app_ranks_weekly.store_app
           FROM frontend.store_app_ranks_weekly
          WHERE ((store_app_ranks_weekly.crawled_date >= '2025-01-01'::date) AND (store_app_ranks_weekly.crawled_date < '2026-01-01'::date))
        ), totals AS (
         SELECT 'h1'::text AS half,
            count(DISTINCT combined_store_apps_companies_2025_h1.store_app) AS total_apps
           FROM adtech.combined_store_apps_companies_2025_h1
          WHERE (combined_store_apps_companies_2025_h1.sdk AND (combined_store_apps_companies_2025_h1.store_app IN ( SELECT limit_apps.store_app
                   FROM limit_apps)))
        UNION ALL
         SELECT 'h2'::text AS half,
            count(DISTINCT combined_store_apps_companies_2025_h2.store_app) AS total_apps
           FROM adtech.combined_store_apps_companies_2025_h2
          WHERE (combined_store_apps_companies_2025_h2.sdk AND (combined_store_apps_companies_2025_h2.store_app IN ( SELECT limit_apps.store_app
                   FROM limit_apps)))
        ), domain_counts AS (
         SELECT 'h1'::text AS half,
            combined_store_apps_companies_2025_h1.ad_domain,
            count(DISTINCT combined_store_apps_companies_2025_h1.store_app) AS app_count
           FROM adtech.combined_store_apps_companies_2025_h1
          WHERE (combined_store_apps_companies_2025_h1.sdk AND (combined_store_apps_companies_2025_h1.store_app IN ( SELECT limit_apps.store_app
                   FROM limit_apps)))
          GROUP BY combined_store_apps_companies_2025_h1.ad_domain
        UNION ALL
         SELECT 'h2'::text AS half,
            combined_store_apps_companies_2025_h2.ad_domain,
            count(DISTINCT combined_store_apps_companies_2025_h2.store_app) AS app_count
           FROM adtech.combined_store_apps_companies_2025_h2
          WHERE (combined_store_apps_companies_2025_h2.sdk AND (combined_store_apps_companies_2025_h2.store_app IN ( SELECT limit_apps.store_app
                   FROM limit_apps)))
          GROUP BY combined_store_apps_companies_2025_h2.ad_domain
        ), shares AS (
         SELECT d.half,
            d.ad_domain,
            d.app_count,
            t.total_apps,
            ((d.app_count)::numeric / (NULLIF(t.total_apps, 0))::numeric) AS pct_share
           FROM (domain_counts d
             JOIN totals t ON ((t.half = d.half)))
        ), shares_h1 AS (
         SELECT shares.half,
            shares.ad_domain,
            shares.app_count,
            shares.total_apps,
            shares.pct_share
           FROM shares
          WHERE (shares.half = 'h1'::text)
        ), shares_h2 AS (
         SELECT shares.half,
            shares.ad_domain,
            shares.app_count,
            shares.total_apps,
            shares.pct_share
           FROM shares
          WHERE (shares.half = 'h2'::text)
        )
 SELECT COALESCE(s2.ad_domain, s1.ad_domain) AS ad_domain,
    s1.app_count AS apps_h1,
    s1.total_apps AS total_apps_h1,
    round((COALESCE(s1.pct_share, (0)::numeric) * (100)::numeric), 4) AS share_h1_pct,
    s2.app_count AS apps_h2,
    s2.total_apps AS total_apps_h2,
    round((COALESCE(s2.pct_share, (0)::numeric) * (100)::numeric), 4) AS share_h2_pct,
    (COALESCE(s2.app_count, (0)::bigint) - COALESCE(s1.app_count, (0)::bigint)) AS net_app_change,
        CASE
            WHEN ((s1.app_count IS NULL) OR (s1.app_count = 0)) THEN 100.00
            ELSE round(((((COALESCE(s2.app_count, (0)::bigint) - s1.app_count))::numeric / (s1.app_count)::numeric) * (100)::numeric), 2)
        END AS app_growth_pct,
    round(((COALESCE(s2.pct_share, (0)::numeric) - COALESCE(s1.pct_share, (0)::numeric)) * (100)::numeric), 6) AS share_change_pp
   FROM (shares_h1 s1
     FULL JOIN shares_h2 s2 ON (((s1.ad_domain)::text = (s2.ad_domain)::text)))
  ORDER BY (round(((COALESCE(s2.pct_share, (0)::numeric) - COALESCE(s1.pct_share, (0)::numeric)) * (100)::numeric), 6)) DESC NULLS LAST
  WITH NO DATA;


ALTER MATERIALIZED VIEW adtech.company_share_change_2025 OWNER TO postgres;

--
-- PostgreSQL database dump complete
--

\unrestrict BgXRyIjKPvCreuAubND2kXoCGnlOr1MDrT5IazYXU07U2qHj7CU38EGnX8Mpdob

