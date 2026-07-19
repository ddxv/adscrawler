--
-- PostgreSQL database dump
--

\restrict kOaCuOAbQkLEbJ7jwhV5Trtt08koxhbsdwmGwhhm0Xzo1toJnzzai42t6GSLLsT

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

--
-- Name: trend_domains; Type: TABLE; Schema: adtech; Owner: postgres
--

CREATE TABLE adtech.trend_domains (
    domain_name character varying,
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
    batch_date date NOT NULL
)
PARTITION BY LIST (batch_date);


ALTER TABLE adtech.trend_domains OWNER TO postgres;

--
-- PostgreSQL database dump complete
--

\unrestrict kOaCuOAbQkLEbJ7jwhV5Trtt08koxhbsdwmGwhhm0Xzo1toJnzzai42t6GSLLsT

