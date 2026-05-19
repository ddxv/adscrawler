--
-- PostgreSQL database dump
--

\restrict t2tuuvIJlgtdCg7ycYKLadeUsoAFh8JB5ZaeJSz0RjE21W9dPZ4fwlvZNX8yUN1

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
-- Name: combined_company_app_history; Type: TABLE; Schema: adtech; Owner: postgres
--

CREATE TABLE adtech.combined_company_app_history (
    ad_domain text,
    store_app bigint,
    company_id bigint,
    parent_id bigint,
    sdk boolean,
    api_call boolean,
    app_ads_direct boolean,
    year bigint,
    quarter bigint
);


ALTER TABLE adtech.combined_company_app_history OWNER TO postgres;

--
-- PostgreSQL database dump complete
--

\unrestrict t2tuuvIJlgtdCg7ycYKLadeUsoAFh8JB5ZaeJSz0RjE21W9dPZ4fwlvZNX8yUN1

