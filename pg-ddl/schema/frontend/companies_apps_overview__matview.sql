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
-- Name: companies_apps_overview; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.companies_apps_overview AS
SELECT DISTINCT
    store_id,
    company_id,
    company_name,
    company_domain,
    category_slug
FROM frontend.store_apps_version_details
WHERE (company_id IS NOT null)
WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.companies_apps_overview OWNER TO postgres;

--
-- Name: companies_apps_overview_idx; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE INDEX companies_apps_overview_idx ON frontend.companies_apps_overview USING btree (
    store_id
);


--
-- Name: companies_apps_overview_unique_idx; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE UNIQUE INDEX companies_apps_overview_unique_idx ON frontend.companies_apps_overview USING btree (
    store_id, company_id, category_slug
);


--
-- PostgreSQL database dump complete
--
