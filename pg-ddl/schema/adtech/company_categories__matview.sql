--
-- PostgreSQL database dump
--

\restrict rsGSEniqgIzDNzMTBL4OGVCrFU9Eh1DlQi2EdDq0xoRjmewOdISYaZfUJKqFaha

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
-- Name: company_categories; Type: MATERIALIZED VIEW; Schema: adtech; Owner: postgres
--

CREATE MATERIALIZED VIEW adtech.company_categories AS
 SELECT DISTINCT c.id AS company_id,
    sc.category_id
   FROM ((adtech.sdk_categories sc
     LEFT JOIN adtech.sdks sd ON ((sc.sdk_id = sd.id)))
     JOIN adtech.companies c ON ((sd.company_id = c.id)))
  WITH NO DATA;


ALTER MATERIALIZED VIEW adtech.company_categories OWNER TO postgres;

--
-- PostgreSQL database dump complete
--

\unrestrict rsGSEniqgIzDNzMTBL4OGVCrFU9Eh1DlQi2EdDq0xoRjmewOdISYaZfUJKqFaha

