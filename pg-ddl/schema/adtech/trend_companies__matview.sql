--
-- PostgreSQL database dump
--

\restrict 4Fy7JKFcIlQEHeAo4dLiKun4kPZcTetRAlbPcZUExZmN8dae7pog9qXrLXB2qMQ

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
-- Name: trend_companies; Type: MATERIALIZED VIEW; Schema: adtech; Owner: postgres
--

CREATE MATERIALIZED VIEW adtech.trend_companies AS
 WITH enriched_raw AS (
         SELECT cdm.company_id,
            h.store_app,
            bool_or(h.sdk) AS sdk,
            bool_or(h.api_call) AS api_call,
            bool_or(h.app_ads_direct) AS app_ads_direct,
            h.year,
            h.quarter,
            sa.store
           FROM ((adtech.combined_domain_app_history h
             LEFT JOIN public.store_apps sa ON ((sa.id = h.store_app)))
             LEFT JOIN adtech.company_domain_mapping cdm ON ((h.domain_id = cdm.domain_id)))
          GROUP BY cdm.company_id, h.store_app, h.year, h.quarter, sa.store
        ), enriched AS (
         SELECT enriched_raw.company_id,
            enriched_raw.store_app,
            enriched_raw.year,
            enriched_raw.quarter,
            enriched_raw.store,
            t.tag_source
           FROM enriched_raw,
            LATERAL ( VALUES ('sdk'::text,enriched_raw.sdk), ('api_call'::text,enriched_raw.api_call), ('app_ads_direct'::text,enriched_raw.app_ads_direct)) t(tag_source, is_active)
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
         SELECT enriched.company_id,
            enriched.year,
            enriched.quarter,
            enriched.store,
            enriched.tag_source,
            count(enriched.store_app) AS total_apps,
            pre_agg.total_apps_in_quarter
           FROM (enriched
             JOIN pre_agg ON (((pre_agg.year = enriched.year) AND (pre_agg.quarter = enriched.quarter) AND (pre_agg.store = enriched.store) AND (pre_agg.tag_source = enriched.tag_source))))
          GROUP BY enriched.company_id, enriched.year, enriched.quarter, enriched.store, enriched.tag_source, pre_agg.total_apps_in_quarter
        ), churned AS (
         SELECT p.company_id,
            p.store,
            p.tag_source,
                CASE
                    WHEN (p.quarter = 4) THEN (p.year + 1)
                    ELSE (p.year)::integer
                END AS year,
                CASE
                    WHEN (p.quarter = 4) THEN (1)::bigint
                    ELSE ((p.quarter + 1))::bigint
                END AS quarter,
            count(p.store_app) AS apps_lost
           FROM (enriched p
             LEFT JOIN enriched c ON (((p.company_id = c.company_id) AND (p.store_app = c.store_app) AND (p.store = c.store) AND (p.tag_source = c.tag_source) AND (((p.quarter = 4) AND (c.year = (p.year + 1)) AND (c.quarter = 1)) OR ((p.quarter < 4) AND (c.year = p.year) AND (c.quarter = (p.quarter + 1)))))))
          WHERE (c.store_app IS NULL)
          GROUP BY p.company_id, p.year, p.quarter, p.store, p.tag_source
        ), added AS (
         SELECT c.company_id,
            c.store,
            c.tag_source,
            c.year,
            c.quarter,
            count(c.store_app) AS apps_added
           FROM (enriched c
             LEFT JOIN enriched p ON (((c.company_id = p.company_id) AND (c.store_app = p.store_app) AND (c.store = p.store) AND (c.tag_source = p.tag_source) AND (((c.quarter = 1) AND (p.year = (c.year - 1)) AND (p.quarter = 4)) OR ((c.quarter > 1) AND (p.year = c.year) AND (p.quarter = (c.quarter - 1)))))))
          WHERE (p.store_app IS NULL)
          GROUP BY c.company_id, c.year, c.quarter, c.store, c.tag_source
        )
 SELECT ad.domain_name AS company_domain,
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
   FROM ((((current_quarter cq
     LEFT JOIN churned ch ON (((cq.company_id = ch.company_id) AND (cq.year = ch.year) AND (cq.quarter = ch.quarter) AND (cq.store = ch.store) AND (cq.tag_source = ch.tag_source))))
     LEFT JOIN added a ON (((cq.company_id = a.company_id) AND (cq.year = a.year) AND (cq.quarter = a.quarter) AND (cq.store = a.store) AND (cq.tag_source = a.tag_source))))
     LEFT JOIN adtech.companies co ON ((cq.company_id = co.id)))
     LEFT JOIN public.domains ad ON ((co.domain_id = ad.id)))
  ORDER BY cq.year, cq.quarter, cq.tag_source, cq.total_apps DESC
  WITH NO DATA;


ALTER MATERIALIZED VIEW adtech.trend_companies OWNER TO postgres;

--
-- Name: idx_trend_companies_concurrent; Type: INDEX; Schema: adtech; Owner: postgres
--

CREATE UNIQUE INDEX idx_trend_companies_concurrent ON adtech.trend_companies USING btree (company_domain, year, quarter, store, tag_source);


--
-- PostgreSQL database dump complete
--

\unrestrict 4Fy7JKFcIlQEHeAo4dLiKun4kPZcTetRAlbPcZUExZmN8dae7pog9qXrLXB2qMQ

