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
-- Name: companies_creative_rankings; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.companies_creative_rankings AS
 WITH creative_rankings AS (
         SELECT COALESCE(ca.phash, ca.md5_hash) AS vhash,
            ca.file_extension,
            ca.store_app_id AS advertiser_store_app_id,
            cr.creative_initial_domain_id,
            cr.creative_host_domain_id,
            cr.additional_ad_domain_ids,
            vcasr.run_at,
            ca.md5_hash
           FROM ((public.creative_records cr
             LEFT JOIN public.creative_assets ca ON ((cr.creative_asset_id = ca.id)))
             LEFT JOIN public.version_code_api_scan_results vcasr ON ((cr.run_id = vcasr.id)))
        ), combined_domains AS (
         SELECT cr.vhash,
            cr.md5_hash,
            cr.file_extension,
            cr.creative_initial_domain_id AS domain_id,
            cr.advertiser_store_app_id,
            cr.run_at
           FROM creative_rankings cr
        UNION
         SELECT cr.vhash,
            cr.md5_hash,
            cr.file_extension,
            cr.creative_host_domain_id,
            cr.advertiser_store_app_id,
            cr.run_at
           FROM creative_rankings cr
        UNION
         SELECT cr.vhash,
            cr.md5_hash,
            cr.file_extension,
            unnest(cr.additional_ad_domain_ids) AS unnest,
            cr.advertiser_store_app_id,
            cr.run_at
           FROM creative_rankings cr
        ), visually_distinct AS (
         SELECT cdm.company_id,
            cd.file_extension,
            cd.advertiser_store_app_id,
            cd.vhash,
            min((cd.md5_hash)::text) AS md5_hash,
            max(cd.run_at) AS last_seen
           FROM (combined_domains cd
             LEFT JOIN adtech.company_domain_mapping cdm ON ((cd.domain_id = cdm.domain_id)))
          GROUP BY cdm.company_id, cd.file_extension, cd.advertiser_store_app_id, cd.vhash
        )
 SELECT DISTINCT vd.company_id,
    vd.md5_hash,
    vd.file_extension,
    ad.domain AS company_domain,
    sa.store_id AS advertiser_store_id,
    sa.icon_url_512,
    vd.last_seen
   FROM (((visually_distinct vd
     LEFT JOIN adtech.companies c ON ((vd.company_id = c.id)))
     LEFT JOIN public.ad_domains ad ON ((c.domain_id = ad.id)))
     LEFT JOIN frontend.store_apps_overview sa ON ((vd.advertiser_store_app_id = sa.id)))
  WHERE (c.id IS NOT NULL)
  ORDER BY vd.last_seen DESC
  WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.companies_creative_rankings OWNER TO postgres;

--
-- PostgreSQL database dump complete
--

