--
-- PostgreSQL database dump
--

\restrict 386dLDp67mqZF8AHuRI3zw5Ra7YItqzMtDWb371DaOJrXVk3bqJTlNd3Cfcksf6

-- Dumped from database version 18.0 (Ubuntu 18.0-1.pgdg24.04+3)
-- Dumped by pg_dump version 18.0 (Ubuntu 18.0-1.pgdg24.04+3)

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
-- Name: combined_store_apps_parent_companies; Type: MATERIALIZED VIEW; Schema: adtech; Owner: postgres
--

CREATE MATERIALIZED VIEW adtech.combined_store_apps_parent_companies AS
 SELECT csac.store_app,
    csac.app_category,
    csac.parent_id AS company_id,
    COALESCE(ad.domain_name, csac.ad_domain) AS ad_domain,
    bool_or(csac.sdk) AS sdk,
    bool_or(csac.api_call) AS api_call,
    bool_or(csac.app_ads_direct) AS app_ads_direct
   FROM ((adtech.combined_store_apps_companies csac
     LEFT JOIN adtech.companies c ON ((csac.parent_id = c.id)))
     LEFT JOIN public.domains ad ON ((c.domain_id = ad.id)))
  WHERE (csac.parent_id IN ( SELECT DISTINCT pc.id
           FROM (adtech.companies pc
             LEFT JOIN adtech.companies c_1 ON ((pc.id = c_1.parent_company_id)))
          WHERE (c_1.id IS NOT NULL)))
  GROUP BY COALESCE(ad.domain_name, csac.ad_domain), csac.store_app, csac.app_category, csac.parent_id
  WITH NO DATA;


ALTER MATERIALIZED VIEW adtech.combined_store_apps_parent_companies OWNER TO postgres;

--
-- Name: idx_combined_store_apps_parent_companies_idx; Type: INDEX; Schema: adtech; Owner: postgres
--

CREATE UNIQUE INDEX idx_combined_store_apps_parent_companies_idx ON adtech.combined_store_apps_parent_companies USING btree (ad_domain, store_app, app_category, company_id);


--
-- PostgreSQL database dump complete
--

\unrestrict 386dLDp67mqZF8AHuRI3zw5Ra7YItqzMtDWb371DaOJrXVk3bqJTlNd3Cfcksf6

