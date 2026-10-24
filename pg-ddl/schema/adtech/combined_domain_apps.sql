--
-- PostgreSQL database dump
--

\restrict Q2y10kYu8OoidUCPt2UCfa7a2jsrv4ipNE1qCvZCN0iK0L4Q76ypy6nvtvph802

-- Dumped from database version 18.6 (Ubuntu 18.6-1.pgdg26.04+2)
-- Dumped by pg_dump version 18.6 (Ubuntu 18.6-1.pgdg26.04+2)

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
-- Name: combined_domain_apps; Type: TABLE; Schema: adtech; Owner: postgres
--

CREATE TABLE adtech.combined_domain_apps (
    domain_id integer NOT NULL,
    store_app integer NOT NULL,
    company_id integer,
    sdk boolean,
    api_call boolean,
    publisher boolean,
    app_ads_direct boolean,
    app_ads_reseller boolean,
    batch_date date NOT NULL
)
PARTITION BY LIST (batch_date);


ALTER TABLE adtech.combined_domain_apps OWNER TO postgres;

--
-- Name: combined_domain_apps combined_domain_apps_pkey; Type: CONSTRAINT; Schema: adtech; Owner: postgres
--

ALTER TABLE ONLY adtech.combined_domain_apps
    ADD CONSTRAINT combined_domain_apps_pkey PRIMARY KEY (domain_id, store_app, batch_date);


--
-- PostgreSQL database dump complete
--

\unrestrict Q2y10kYu8OoidUCPt2UCfa7a2jsrv4ipNE1qCvZCN0iK0L4Q76ypy6nvtvph802

