--
-- PostgreSQL database dump
--

\restrict ZaHnF2UCymdkwRd77k2wJoCv6LFuprOP6MXAUgwNZFybDGfKvxmfPkbacxP3ktr

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
-- Name: store_app_api_companies; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.store_app_api_companies AS
 WITH latest_run_per_app AS (
         SELECT DISTINCT ON (saac_1.store_app) saac_1.store_app,
            saac_1.run_id
           FROM (public.api_calls saac_1
             JOIN public.version_code_api_scan_results vcasr ON ((saac_1.run_id = vcasr.id)))
          ORDER BY saac_1.store_app, vcasr.run_at DESC
        )
 SELECT DISTINCT sa.store_id,
    ac.tld_url AS company_domain,
    c.id AS company_id,
    c.name AS company_name,
    co.alpha2 AS country
   FROM ((((((((((latest_run_per_app lrpa
     LEFT JOIN public.api_calls ac ON ((lrpa.run_id = ac.run_id)))
     LEFT JOIN public.domains ad ON ((ac.tld_url = (ad.domain_name)::text)))
     LEFT JOIN public.store_apps sa ON ((ac.store_app = sa.id)))
     LEFT JOIN adtech.company_domain_mapping cdm ON ((ad.id = cdm.domain_id)))
     LEFT JOIN adtech.companies c ON ((cdm.company_id = c.id)))
     LEFT JOIN public.domains cad ON ((c.domain_id = cad.id)))
     LEFT JOIN adtech.companies pc ON ((c.parent_company_id = pc.id)))
     LEFT JOIN public.domains pcad ON ((pc.domain_id = pcad.id)))
     LEFT JOIN public.ip_geo_snapshots igs ON ((ac.ip_geo_snapshot_id = igs.id)))
     LEFT JOIN public.countries co ON ((igs.country_id = co.id)))
  ORDER BY sa.store_id DESC
  WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.store_app_api_companies OWNER TO postgres;

--
-- PostgreSQL database dump complete
--

\unrestrict ZaHnF2UCymdkwRd77k2wJoCv6LFuprOP6MXAUgwNZFybDGfKvxmfPkbacxP3ktr

