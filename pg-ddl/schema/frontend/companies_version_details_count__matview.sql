--
-- PostgreSQL database dump
--

\restrict wtMQ1mPgW1um64pkUL18kx7hCCgdAHECy9GPx7JrPXrF9gGPEapnpIOcodKEG32

-- Dumped from database version 17.6 (Ubuntu 17.6-2.pgdg24.04+1)
-- Dumped by pg_dump version 17.6 (Ubuntu 17.6-2.pgdg24.04+1)

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
-- Name: companies_version_details_count; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.companies_version_details_count AS
SELECT
    savd.store,
    savd.company_name,
    savd.company_domain,
    vs.xml_path,
    vs.value_name,
    count(DISTINCT savd.store_id) AS app_count
FROM (
    frontend.store_apps_version_details AS savd
    LEFT JOIN public.version_strings AS vs ON ((savd.version_string_id = vs.id))
)
GROUP BY
    savd.store,
    savd.company_name,
    savd.company_domain,
    vs.xml_path,
    vs.value_name
ORDER BY (count(DISTINCT savd.store_id)) DESC
LIMIT 1000
WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.companies_version_details_count OWNER TO postgres;

--
-- Name: companies_apps_version_details_count_unique_idx; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE UNIQUE INDEX companies_apps_version_details_count_unique_idx ON frontend.companies_version_details_count USING btree (
    store, company_name, company_domain, xml_path, value_name
);


--
-- PostgreSQL database dump complete
--

\unrestrict wtMQ1mPgW1um64pkUL18kx7hCCgdAHECy9GPx7JrPXrF9gGPEapnpIOcodKEG32
