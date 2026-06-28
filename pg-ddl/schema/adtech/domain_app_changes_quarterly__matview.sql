--
-- PostgreSQL database dump
--

\restrict XcsVz6YUAWuDq8Ij2oC46iM7eo74ZhvwhiG7zY3KoGl7svSqK3FTcVZDm0UX72d

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
-- Name: domain_app_changes_quarterly; Type: MATERIALIZED VIEW; Schema: adtech; Owner: postgres
--

CREATE MATERIALIZED VIEW adtech.domain_app_changes_quarterly AS
 WITH enriched_raw AS (
         SELECT h.domain_id,
            h.store_app,
            h.sdk,
            h.api_call,
            h.app_ads_direct,
            h.year,
            h.quarter,
            sa.release_date
           FROM (adtech.combined_domain_app_history h
             LEFT JOIN public.store_apps sa ON ((sa.id = h.store_app)))
        ), enriched AS (
         SELECT enriched_raw.domain_id,
            enriched_raw.store_app,
            enriched_raw.year,
            enriched_raw.quarter,
            enriched_raw.release_date,
            t.tag_source
           FROM enriched_raw,
            LATERAL ( VALUES ('sdk'::text,enriched_raw.sdk), ('api_call'::text,enriched_raw.api_call), ('app_ads_direct'::text,enriched_raw.app_ads_direct)) t(tag_source, is_active)
          WHERE (t.is_active = true)
        ), max_period AS (
         SELECT max(((enriched.year * 10) + enriched.quarter)) AS max_yq
           FROM enriched
        ), app_tag_first_seen AS (
         SELECT enriched.store_app,
            enriched.tag_source,
            min(((enriched.year * 10) + enriched.quarter)) AS first_seen_key
           FROM enriched
          GROUP BY enriched.store_app, enriched.tag_source
        ), added AS (
         SELECT c.domain_id,
            c.store_app,
            c.tag_source,
            c.year,
            c.quarter,
                CASE
                    WHEN ((((c.year * 10) + c.quarter) = af.first_seen_key) AND (NOT ((c.release_date >= make_date((c.year)::integer, (((c.quarter - 1) * 3) + 1), 1)) AND (c.release_date < (make_date((c.year)::integer, (((c.quarter - 1) * 3) + 1), 1) + '3 mons'::interval))))) THEN 'added_initial'::adtech.change_status_enum
                    ELSE 'added'::adtech.change_status_enum
                END AS status
           FROM ((enriched c
             LEFT JOIN enriched p ON (((c.domain_id = p.domain_id) AND (c.store_app = p.store_app) AND (c.tag_source = p.tag_source) AND (((c.quarter = 1) AND (p.year = (c.year - 1)) AND (p.quarter = 4)) OR ((c.quarter > 1) AND (p.year = c.year) AND (p.quarter = (c.quarter - 1)))))))
             JOIN app_tag_first_seen af ON (((c.store_app = af.store_app) AND (c.tag_source = af.tag_source))))
          WHERE (p.store_app IS NULL)
        ), removed AS (
         SELECT p.domain_id,
            p.store_app,
            p.tag_source,
                CASE
                    WHEN (p.quarter = 4) THEN (p.year + 1)
                    ELSE (p.year)::integer
                END AS year,
                CASE
                    WHEN (p.quarter = 4) THEN 1
                    ELSE (p.quarter + 1)
                END AS quarter,
            'removed'::adtech.change_status_enum AS status
           FROM ((enriched p
             CROSS JOIN max_period mp)
             LEFT JOIN enriched c ON (((p.domain_id = c.domain_id) AND (p.store_app = c.store_app) AND (p.tag_source = c.tag_source) AND (((p.quarter = 4) AND (c.year = (p.year + 1)) AND (c.quarter = 1)) OR ((p.quarter < 4) AND (c.year = p.year) AND (c.quarter = (p.quarter + 1)))))))
          WHERE ((c.store_app IS NULL) AND (((
                CASE
                    WHEN (p.quarter = 4) THEN (p.year + 1)
                    ELSE (p.year)::integer
                END * 10) +
                CASE
                    WHEN (p.quarter = 4) THEN 1
                    ELSE (p.quarter + 1)
                END) <= mp.max_yq))
        )
 SELECT added.domain_id,
    added.store_app,
    added.tag_source,
    added.year,
    added.quarter,
    added.status
   FROM added
UNION ALL
 SELECT removed.domain_id,
    removed.store_app,
    removed.tag_source,
    removed.year,
    removed.quarter,
    removed.status
   FROM removed
  ORDER BY 4, 5, 1, 3, 6
  WITH NO DATA;


ALTER MATERIALIZED VIEW adtech.domain_app_changes_quarterly OWNER TO postgres;

--
-- Name: idx_domain_app_changes_quarterly_lookup; Type: INDEX; Schema: adtech; Owner: postgres
--

CREATE INDEX idx_domain_app_changes_quarterly_lookup ON adtech.domain_app_changes_quarterly USING btree (year, quarter, domain_id);


--
-- Name: idx_domain_app_changes_quarterly_pk; Type: INDEX; Schema: adtech; Owner: postgres
--

CREATE UNIQUE INDEX idx_domain_app_changes_quarterly_pk ON adtech.domain_app_changes_quarterly USING btree (domain_id, store_app, year, quarter, tag_source, status);


--
-- PostgreSQL database dump complete
--

\unrestrict XcsVz6YUAWuDq8Ij2oC46iM7eo74ZhvwhiG7zY3KoGl7svSqK3FTcVZDm0UX72d

