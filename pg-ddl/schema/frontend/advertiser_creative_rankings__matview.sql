--
-- PostgreSQL database dump
--

\restrict KLOO3vW2Qk9R0ky0fShzb7FGELp8LPFucsRvZlm2gI8wF7eox2K7vpdL308NCu1

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
-- Name: advertiser_creative_rankings; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.advertiser_creative_rankings AS
 WITH adv_mmp AS (
         SELECT DISTINCT cr_1.advertiser_store_app_id,
            cr_1.mmp_domain_id,
            ad.domain_name AS mmp_domain
           FROM (public.creative_records cr_1
             LEFT JOIN public.domains ad ON ((cr_1.mmp_domain_id = ad.id)))
          WHERE ((cr_1.mmp_domain_id IS NOT NULL) AND (cr_1.advertiser_store_app_id IS NOT NULL))
        ), ad_network_domain_ids AS (
         SELECT cr_1.advertiser_store_app_id,
            COALESCE(icp.domain_id, ic.domain_id) AS domain_id
           FROM (((public.creative_records cr_1
             JOIN adtech.company_domain_mapping icdm ON ((cr_1.creative_initial_domain_id = icdm.domain_id)))
             LEFT JOIN adtech.companies ic ON ((icdm.company_id = ic.id)))
             LEFT JOIN adtech.companies icp ON ((ic.parent_company_id = icp.id)))
        UNION
         SELECT cr_1.advertiser_store_app_id,
            COALESCE(hcp.domain_id, hc.domain_id) AS domain_id
           FROM (((public.creative_records cr_1
             JOIN adtech.company_domain_mapping hcdm ON ((cr_1.creative_host_domain_id = hcdm.domain_id)))
             LEFT JOIN adtech.companies hc ON ((hcdm.company_id = hc.id)))
             LEFT JOIN adtech.companies hcp ON ((hc.parent_company_id = hcp.id)))
        ), ad_network_domains AS (
         SELECT adi.advertiser_store_app_id,
            ad.domain_name AS ad_network_domain
           FROM (ad_network_domain_ids adi
             LEFT JOIN public.domains ad ON ((adi.domain_id = ad.id)))
        ), creative_rankings AS (
         SELECT ca_1.md5_hash,
            ca_1.file_extension,
            cr_1.advertiser_store_app_id,
            vcasr_1.run_at,
            row_number() OVER (PARTITION BY cr_1.advertiser_store_app_id ORDER BY vcasr_1.run_at DESC) AS rn
           FROM (((public.creative_records cr_1
             LEFT JOIN public.creative_assets ca_1 ON ((cr_1.creative_asset_id = ca_1.id)))
             LEFT JOIN public.api_calls ac_1 ON ((cr_1.api_call_id = ac_1.id)))
             LEFT JOIN public.version_code_api_scan_results vcasr_1 ON ((ac_1.run_id = vcasr_1.id)))
        )
 SELECT saa.name AS advertiser_name,
    saa.store_id AS advertiser_store_id,
    saa.icon_url_100 AS advertiser_icon_url_100,
    saa.icon_url_512 AS advertiser_icon_url_512,
    saa.category AS advertiser_category,
    saa.installs AS advertiser_installs,
    saa.rating,
    saa.rating_count,
    saa.installs_sum_1w,
    saa.installs_sum_4w,
    count(DISTINCT ca.md5_hash) AS unique_creatives,
    count(DISTINCT ac.store_app) AS unique_publishers,
    min(vcasr.run_at) AS first_seen,
    max(vcasr.run_at) AS last_seen,
    array_agg(DISTINCT ca.file_extension) AS file_types,
    array_agg(DISTINCT adis.ad_network_domain) AS ad_network_domains,
    avg(sap.installs) AS avg_publisher_installs,
    NULLIF(array_agg(DISTINCT adv_mmp.mmp_domain) FILTER (WHERE (adv_mmp.mmp_domain IS NOT NULL)), '{}'::character varying[]) AS mmp_domains,
    ARRAY( SELECT crk.md5_hash
           FROM creative_rankings crk
          WHERE ((crk.advertiser_store_app_id = saa.id) AND (crk.rn <= 5))
          ORDER BY crk.rn) AS top_md5_hashes
   FROM (((((((public.creative_records cr
     LEFT JOIN public.creative_assets ca ON ((cr.creative_asset_id = ca.id)))
     LEFT JOIN public.api_calls ac ON ((cr.api_call_id = ac.id)))
     LEFT JOIN frontend.store_apps_overview sap ON ((ac.store_app = sap.id)))
     LEFT JOIN frontend.store_apps_overview saa ON ((cr.advertiser_store_app_id = saa.id)))
     LEFT JOIN public.version_code_api_scan_results vcasr ON ((ac.run_id = vcasr.id)))
     LEFT JOIN adv_mmp ON ((cr.advertiser_store_app_id = adv_mmp.advertiser_store_app_id)))
     LEFT JOIN ad_network_domains adis ON ((cr.advertiser_store_app_id = adis.advertiser_store_app_id)))
  WHERE (cr.advertiser_store_app_id IS NOT NULL)
  GROUP BY saa.name, saa.store_id, saa.icon_url_512, saa.category, saa.installs, saa.id, saa.icon_url_100, saa.rating, saa.rating_count, saa.installs_sum_1w, saa.installs_sum_4w
  ORDER BY (count(DISTINCT ca.md5_hash)) DESC
  WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.advertiser_creative_rankings OWNER TO postgres;

--
-- PostgreSQL database dump complete
--

\unrestrict KLOO3vW2Qk9R0ky0fShzb7FGELp8LPFucsRvZlm2gI8wF7eox2K7vpdL308NCu1

