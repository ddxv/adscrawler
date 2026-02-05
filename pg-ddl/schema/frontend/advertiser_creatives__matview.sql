--
-- PostgreSQL database dump
--

\restrict CiB6aJyc7ez3uqqaJfN8zVgdRKeXYe8GVHwr9iWwoWgkre2ZQwgY8vrf16HRmQk

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
-- Name: advertiser_creatives; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.advertiser_creatives AS
 SELECT saa.store_id AS advertiser_store_id,
    ac1.run_id,
    vcasr.run_at,
    sap.name AS pub_name,
    saa.name AS adv_name,
    sap.store_id AS pub_store_id,
    saa.store_id AS adv_store_id,
    hd.domain_name AS host_domain,
    hc.name AS host_domain_company_name,
    ad.domain_name AS ad_domain,
    ac.name AS ad_domain_company_name,
    ca.md5_hash,
    ca.file_extension,
    sap.icon_url_100 AS pub_icon_url_100,
    saa.icon_url_100 AS adv_icon_url_100,
    sap.icon_url_512 AS pub_icon_url_512,
    saa.icon_url_512 AS adv_icon_url_512,
    mmp.name AS mmp_name,
    mmpd.domain_name AS mmp_domain,
    cr.mmp_urls,
    COALESCE(hcd.domain_name, hd.domain_name) AS host_domain_company_domain,
    COALESCE(acd.domain_name, ad.domain_name) AS ad_domain_company_domain,
    COALESCE(ca.phash, ca.md5_hash) AS vhash,
    ( SELECT COALESCE(array_agg(domains.domain_name), '{}'::character varying[]) AS array_agg
           FROM public.domains
          WHERE (domains.id = ANY (cr.additional_ad_domain_ids))) AS additional_ad_domain_urls
   FROM ((((((((((((((((public.creative_records cr
     LEFT JOIN public.creative_assets ca ON ((cr.creative_asset_id = ca.id)))
     LEFT JOIN public.api_calls ac1 ON ((cr.api_call_id = ac1.id)))
     LEFT JOIN frontend.store_apps_overview sap ON ((ac1.store_app = sap.id)))
     LEFT JOIN frontend.store_apps_overview saa ON ((cr.advertiser_store_app_id = saa.id)))
     LEFT JOIN public.version_code_api_scan_results vcasr ON ((ac1.run_id = vcasr.id)))
     LEFT JOIN public.domains hd ON ((cr.creative_host_domain_id = hd.id)))
     LEFT JOIN public.domains ad ON ((cr.creative_initial_domain_id = ad.id)))
     LEFT JOIN adtech.company_domain_mapping hcdm ON ((hd.id = hcdm.domain_id)))
     LEFT JOIN adtech.company_domain_mapping acdm ON ((ad.id = acdm.domain_id)))
     LEFT JOIN adtech.companies hc ON ((hcdm.company_id = hc.id)))
     LEFT JOIN adtech.companies ac ON ((acdm.company_id = ac.id)))
     LEFT JOIN public.domains hcd ON ((hc.domain_id = hcd.id)))
     LEFT JOIN public.domains acd ON ((ac.domain_id = acd.id)))
     LEFT JOIN adtech.company_domain_mapping cdm ON ((cr.mmp_domain_id = cdm.domain_id)))
     LEFT JOIN adtech.companies mmp ON ((cdm.company_id = mmp.id)))
     LEFT JOIN public.domains mmpd ON ((cr.mmp_domain_id = mmpd.id)))
  WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.advertiser_creatives OWNER TO postgres;

--
-- PostgreSQL database dump complete
--

\unrestrict CiB6aJyc7ez3uqqaJfN8zVgdRKeXYe8GVHwr9iWwoWgkre2ZQwgY8vrf16HRmQk

