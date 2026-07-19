--
-- PostgreSQL database dump
--

\restrict ZClafbCBdtJnXU2f2b0CkZPNVSLarbtG3Nnvd9w6rBxa9mfRgARcweQzPriaTAl

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
-- Name: trend_parent_companies_20260719; Type: TABLE; Schema: adtech; Owner: postgres
--

CREATE TABLE adtech.trend_parent_companies_20260719 (
    company_domain character varying,
    year smallint,
    quarter smallint,
    store integer,
    tag_source text,
    total_apps bigint,
    total_apps_in_quarter bigint,
    apps_lost bigint,
    apps_added bigint,
    pct_market_share numeric,
    pct_apps_added numeric,
    pct_apps_lost numeric,
    batch_date date CONSTRAINT trend_parent_companies_batch_date_not_null NOT NULL,
    CONSTRAINT trend_parent_companies_20260719_batch_date_check CHECK ((batch_date = '2026-07-19'::date))
);


ALTER TABLE adtech.trend_parent_companies_20260719 OWNER TO postgres;

--
-- Name: trend_parent_companies_20260719; Type: TABLE ATTACH; Schema: adtech; Owner: postgres
--

ALTER TABLE ONLY adtech.trend_parent_companies ATTACH PARTITION adtech.trend_parent_companies_20260719 FOR VALUES IN ('2026-07-19');


--
-- PostgreSQL database dump complete
--

\unrestrict ZClafbCBdtJnXU2f2b0CkZPNVSLarbtG3Nnvd9w6rBxa9mfRgARcweQzPriaTAl

