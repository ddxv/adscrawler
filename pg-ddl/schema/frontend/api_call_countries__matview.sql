--
-- PostgreSQL database dump
--

\restrict ByxEie7uWg2oMGKU9MU0k7ppBOlEgqakhWbFmcKaN3Zvk1n4vyrbglT4nSHsr9G

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
-- Name: api_call_countries; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.api_call_countries AS
 WITH latest_run_per_app AS (
         SELECT DISTINCT ON (ac.store_app) ac.store_app,
            ac.run_id
           FROM ((public.api_calls ac
             JOIN public.version_code_api_scan_results vcasr ON ((ac.run_id = vcasr.id)))
             JOIN public.ip_geo_snapshots igs ON ((ac.ip_geo_snapshot_id = igs.id)))
          WHERE (igs.country_id IS NOT NULL)
          ORDER BY ac.store_app, vcasr.run_at DESC
        ), filtered_calls AS (
         SELECT ac.store_app,
            ac.tld_url,
            ac.url,
            igs.country_id,
            igs.city_name,
            igs.org
           FROM ((public.api_calls ac
             JOIN latest_run_per_app lra ON (((ac.store_app = lra.store_app) AND (ac.run_id = lra.run_id))))
             JOIN public.ip_geo_snapshots igs ON ((ac.ip_geo_snapshot_id = igs.id)))
        ), cleaned_calls AS (
         SELECT filtered_calls.store_app,
            filtered_calls.tld_url,
            filtered_calls.country_id,
            filtered_calls.city_name,
            filtered_calls.org,
            regexp_replace(regexp_replace(regexp_replace(filtered_calls.url, '^https?://'::text, ''::text), '\?.*$'::text, ''::text), '^(([^/]+/){0,2}[^/]+).*$'::text, '\1'::text) AS short_url
           FROM filtered_calls
        )
 SELECT ca.tld_url,
    co.alpha2 AS country,
    ca.org,
    COALESCE(cad.domain_name, (ca.tld_url)::character varying) AS company_domain,
    COALESCE(pcad.domain_name, COALESCE(cad.domain_name, (ca.tld_url)::character varying)) AS parent_company_domain,
    count(DISTINCT ca.store_app) AS store_app_count
   FROM (((((((cleaned_calls ca
     LEFT JOIN public.domains ad ON ((ca.tld_url = (ad.domain_name)::text)))
     LEFT JOIN adtech.company_domain_mapping cdm ON ((ad.id = cdm.domain_id)))
     LEFT JOIN adtech.companies c ON ((cdm.company_id = c.id)))
     LEFT JOIN public.domains cad ON ((c.domain_id = cad.id)))
     LEFT JOIN adtech.companies pc ON ((c.parent_company_id = pc.id)))
     LEFT JOIN public.domains pcad ON ((pc.domain_id = pcad.id)))
     LEFT JOIN public.countries co ON ((ca.country_id = co.id)))
  GROUP BY COALESCE(cad.domain_name, (ca.tld_url)::character varying), COALESCE(pcad.domain_name, COALESCE(cad.domain_name, (ca.tld_url)::character varying)), ca.tld_url, co.alpha2, ca.org
  ORDER BY (count(DISTINCT ca.store_app)) DESC
  WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.api_call_countries OWNER TO postgres;

--
-- Name: api_call_countries_unique; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE UNIQUE INDEX api_call_countries_unique ON frontend.api_call_countries USING btree (company_domain, parent_company_domain, tld_url, country, org);


--
-- PostgreSQL database dump complete
--

\unrestrict ByxEie7uWg2oMGKU9MU0k7ppBOlEgqakhWbFmcKaN3Zvk1n4vyrbglT4nSHsr9G

