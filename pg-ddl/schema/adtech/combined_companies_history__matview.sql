--
-- PostgreSQL database dump
--

\restrict 3gborAKf6YwoaNDQVALZJMycMVTZdkimhQ2lRh4q5dIzCFKFRvps3hJsONUDZ92

-- Dumped from database version 18.3 (Ubuntu 18.3-1.pgdg24.04+1)
-- Dumped by pg_dump version 18.3 (Ubuntu 18.3-1.pgdg24.04+1)

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
-- Name: combined_companies_history; Type: MATERIALIZED VIEW; Schema: adtech; Owner: postgres
--

CREATE MATERIALIZED VIEW adtech.combined_companies_history AS
 WITH enriched_raw AS (
         SELECT h.ad_domain,
            h.store_app,
            h.company_id,
            h.parent_id,
            h.sdk,
            h.api_call,
            h.app_ads_direct,
            h.year,
            h.quarter,
            sa.store
           FROM (adtech.combined_company_app_history h
             LEFT JOIN public.store_apps sa ON ((sa.id = h.store_app)))
        ), enriched AS (
         SELECT enriched_raw.ad_domain,
            enriched_raw.store_app,
            enriched_raw.company_id,
            enriched_raw.parent_id,
            enriched_raw.year,
            enriched_raw.quarter,
            enriched_raw.store,
            t.tag_source
           FROM enriched_raw,
            LATERAL ( VALUES ('sdk_api'::text,(enriched_raw.sdk OR enriched_raw.api_call)), ('app_ads_direct'::text,enriched_raw.app_ads_direct)) t(tag_source, is_active)
          WHERE (t.is_active = true)
        ), pre_agg AS (
         SELECT enriched.year,
            enriched.quarter,
            enriched.store,
            enriched.tag_source,
            count(DISTINCT enriched.store_app) AS total_apps_in_quarter
           FROM enriched
          GROUP BY enriched.year, enriched.quarter, enriched.store, enriched.tag_source
        ), current_quarter AS (
         SELECT enriched.ad_domain,
            enriched.company_id,
            enriched.parent_id,
            enriched.year,
            enriched.quarter,
            enriched.store,
            enriched.tag_source,
            count(enriched.store_app) AS total_apps,
            pre_agg.total_apps_in_quarter
           FROM (enriched
             JOIN pre_agg ON (((pre_agg.year = enriched.year) AND (pre_agg.quarter = enriched.quarter) AND (pre_agg.store = enriched.store) AND (pre_agg.tag_source = enriched.tag_source))))
          GROUP BY enriched.ad_domain, enriched.company_id, enriched.parent_id, enriched.year, enriched.quarter, enriched.store, enriched.tag_source, pre_agg.total_apps_in_quarter
        ), churned AS (
         SELECT p.ad_domain,
            p.company_id,
            p.parent_id,
            p.store,
            p.tag_source,
                CASE
                    WHEN (p.quarter = 4) THEN (p.year + 1)
                    ELSE p.year
                END AS year,
                CASE
                    WHEN (p.quarter = 4) THEN (1)::bigint
                    ELSE (p.quarter + 1)
                END AS quarter,
            count(p.store_app) AS apps_lost
           FROM (enriched p
             LEFT JOIN enriched c ON (((p.ad_domain = c.ad_domain) AND (p.company_id = c.company_id) AND (p.parent_id = c.parent_id) AND (p.store_app = c.store_app) AND (p.store = c.store) AND (p.tag_source = c.tag_source) AND (((p.quarter = 4) AND (c.year = (p.year + 1)) AND (c.quarter = 1)) OR ((p.quarter < 4) AND (c.year = p.year) AND (c.quarter = (p.quarter + 1)))))))
          WHERE (c.store_app IS NULL)
          GROUP BY p.ad_domain, p.company_id, p.parent_id, p.year, p.quarter, p.store, p.tag_source
        ), added AS (
         SELECT c.ad_domain,
            c.company_id,
            c.parent_id,
            c.store,
            c.tag_source,
            c.year,
            c.quarter,
            count(c.store_app) AS apps_added
           FROM (enriched c
             LEFT JOIN enriched p ON (((c.ad_domain = p.ad_domain) AND (c.company_id = p.company_id) AND (c.parent_id = p.parent_id) AND (c.store_app = p.store_app) AND (c.store = p.store) AND (c.tag_source = p.tag_source) AND (((c.quarter = 1) AND (p.year = (c.year - 1)) AND (p.quarter = 4)) OR ((c.quarter > 1) AND (p.year = c.year) AND (p.quarter = (c.quarter - 1)))))))
          WHERE (p.store_app IS NULL)
          GROUP BY c.ad_domain, c.company_id, c.parent_id, c.year, c.quarter, c.store, c.tag_source
        )
 SELECT cq.ad_domain,
    cq.company_id,
    cq.parent_id,
    cq.year,
    cq.quarter,
    cq.store,
    cq.tag_source,
    cq.total_apps,
    cq.total_apps_in_quarter,
    COALESCE(ch.apps_lost, (0)::bigint) AS apps_lost,
    COALESCE(a.apps_added, (0)::bigint) AS apps_added,
    round((((cq.total_apps)::numeric * 100.0) / NULLIF((cq.total_apps_in_quarter)::numeric, (0)::numeric)), 5) AS pct_market_share,
    round((((COALESCE(a.apps_added, (0)::bigint))::numeric * 100.0) / (NULLIF((cq.total_apps - COALESCE(a.apps_added, (0)::bigint)), 0))::numeric), 2) AS pct_apps_added,
    round((((COALESCE(ch.apps_lost, (0)::bigint))::numeric * 100.0) / (NULLIF((cq.total_apps + COALESCE(ch.apps_lost, (0)::bigint)), 0))::numeric), 2) AS pct_apps_lost
   FROM ((current_quarter cq
     LEFT JOIN churned ch ON (((cq.ad_domain = ch.ad_domain) AND (cq.company_id = ch.company_id) AND (cq.parent_id = ch.parent_id) AND (cq.year = ch.year) AND (cq.quarter = ch.quarter) AND (cq.store = ch.store) AND (cq.tag_source = ch.tag_source))))
     LEFT JOIN added a ON (((cq.ad_domain = a.ad_domain) AND (cq.company_id = a.company_id) AND (cq.parent_id = a.parent_id) AND (cq.year = a.year) AND (cq.quarter = a.quarter) AND (cq.store = a.store) AND (cq.tag_source = a.tag_source))))
  ORDER BY cq.year, cq.quarter, cq.tag_source, cq.total_apps DESC
  WITH NO DATA;


ALTER MATERIALIZED VIEW adtech.combined_companies_history OWNER TO postgres;

--
-- PostgreSQL database dump complete
--

\unrestrict 3gborAKf6YwoaNDQVALZJMycMVTZdkimhQ2lRh4q5dIzCFKFRvps3hJsONUDZ92

