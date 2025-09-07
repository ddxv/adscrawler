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
-- Name: advertiser_creative_rankings; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.advertiser_creative_rankings AS
 WITH adv_mmp AS (
         SELECT DISTINCT ca_1.store_app_id AS advertiser_store_app_id,
            cr_1.mmp_domain_id,
            ad.domain AS mmp_domain
           FROM ((public.creative_records cr_1
             LEFT JOIN public.creative_assets ca_1 ON ((cr_1.creative_asset_id = ca_1.id)))
             LEFT JOIN public.ad_domains ad ON ((cr_1.mmp_domain_id = ad.id)))
          WHERE (cr_1.mmp_domain_id IS NOT NULL)
        ), creative_rankings AS (
         SELECT ca_1.md5_hash,
            ca_1.file_extension,
            ca_1.store_app_id AS advertiser_store_app_id,
            vcasr_1.run_at,
            row_number() OVER (PARTITION BY ca_1.store_app_id ORDER BY vcasr_1.run_at DESC) AS rn
           FROM ((public.creative_records cr_1
             LEFT JOIN public.creative_assets ca_1 ON ((cr_1.creative_asset_id = ca_1.id)))
             LEFT JOIN public.version_code_api_scan_results vcasr_1 ON ((cr_1.run_id = vcasr_1.id)))
        )
 SELECT saa.name AS advertiser_name,
    saa.store_id AS advertiser_store_id,
    saa.icon_url_512 AS advertiser_icon_url_512,
    saa.category AS advertiser_category,
    saa.installs AS advertiser_installs,
    count(DISTINCT ca.md5_hash) AS unique_creatives,
    count(DISTINCT cr.store_app_pub_id) AS unique_publishers,
    min(vcasr.run_at) AS first_seen,
    max(vcasr.run_at) AS last_seen,
    array_agg(DISTINCT ca.file_extension) AS file_types,
    avg(sap.installs) AS avg_publisher_installs,
    array_agg(DISTINCT adv_mmp.mmp_domain) AS mmp_domains,
    ARRAY( SELECT crk.md5_hash
           FROM creative_rankings crk
          WHERE ((crk.advertiser_store_app_id = saa.id) AND (crk.rn <= 5))
          ORDER BY crk.rn) AS top_md5_hashes
   FROM (((((public.creative_records cr
     LEFT JOIN public.creative_assets ca ON ((cr.creative_asset_id = ca.id)))
     LEFT JOIN frontend.store_apps_overview sap ON ((cr.store_app_pub_id = sap.id)))
     LEFT JOIN frontend.store_apps_overview saa ON ((ca.store_app_id = saa.id)))
     LEFT JOIN public.version_code_api_scan_results vcasr ON ((cr.run_id = vcasr.id)))
     LEFT JOIN adv_mmp ON ((ca.store_app_id = adv_mmp.advertiser_store_app_id)))
  GROUP BY saa.name, saa.store_id, saa.icon_url_512, saa.category, saa.installs, saa.id
  ORDER BY (count(DISTINCT ca.md5_hash)) DESC
  WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.advertiser_creative_rankings OWNER TO postgres;

--
-- PostgreSQL database dump complete
--

