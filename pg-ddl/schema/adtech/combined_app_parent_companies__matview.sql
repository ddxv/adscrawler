--
-- PostgreSQL database dump
--

\restrict r82rzUMsEKN1eQw47XxGSabigAnHpenSW2DsLBAWbQaJmypbjk77rz4fXQa0KJ2

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
-- Name: combined_app_parent_companies; Type: MATERIALIZED VIEW; Schema: adtech; Owner: postgres
--

CREATE MATERIALIZED VIEW adtech.combined_app_parent_companies AS
 WITH child_companies AS (
         SELECT csac.store_app,
            c.parent_company_id,
            csac.sdk,
            csac.api_call,
            csac.publisher,
            csac.app_ads_direct,
            csac.app_ads_reseller
           FROM (adtech.combined_app_companies csac
             JOIN adtech.companies c ON ((csac.company_id = c.id)))
          WHERE (c.parent_company_id IS NOT NULL)
        ), parent_companies_direct AS (
         SELECT csac.store_app,
            csac.company_id AS parent_company_id,
            csac.sdk,
            csac.api_call,
            csac.publisher,
            csac.app_ads_direct,
            csac.app_ads_reseller
           FROM (adtech.combined_app_companies csac
             JOIN adtech.companies c ON ((csac.company_id = c.id)))
          WHERE ((c.parent_company_id IS NULL) AND (EXISTS ( SELECT 1
                   FROM adtech.companies child
                  WHERE (child.parent_company_id = csac.company_id))))
        )
 SELECT store_app,
    parent_company_id AS company_id,
    bool_or(sdk) AS sdk,
    bool_or(api_call) AS api_call,
    bool_or(publisher) AS publisher,
    bool_or(app_ads_direct) AS app_ads_direct,
    bool_or(app_ads_reseller) AS app_ads_reseller
   FROM ( SELECT child_companies.store_app,
            child_companies.parent_company_id,
            child_companies.sdk,
            child_companies.api_call,
            child_companies.publisher,
            child_companies.app_ads_direct,
            child_companies.app_ads_reseller
           FROM child_companies
        UNION ALL
         SELECT parent_companies_direct.store_app,
            parent_companies_direct.parent_company_id,
            parent_companies_direct.sdk,
            parent_companies_direct.api_call,
            parent_companies_direct.publisher,
            parent_companies_direct.app_ads_direct,
            parent_companies_direct.app_ads_reseller
           FROM parent_companies_direct) combined
  GROUP BY store_app, parent_company_id
  WITH NO DATA;


ALTER MATERIALIZED VIEW adtech.combined_app_parent_companies OWNER TO postgres;

--
-- Name: idx_combined_app_parent_companies_idx; Type: INDEX; Schema: adtech; Owner: postgres
--

CREATE UNIQUE INDEX idx_combined_app_parent_companies_idx ON adtech.combined_app_parent_companies USING btree (store_app, company_id);


--
-- PostgreSQL database dump complete
--

\unrestrict r82rzUMsEKN1eQw47XxGSabigAnHpenSW2DsLBAWbQaJmypbjk77rz4fXQa0KJ2

