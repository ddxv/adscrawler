--
-- PostgreSQL database dump
--

\restrict raMiEzR70NRz7aWeSf1dn4nRc3ZOPuA6jsTFbKvf8RpHXWDDuXEc09WkhPhh1IF

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
-- Name: domain_app_changes_quarterly; Type: TABLE; Schema: adtech; Owner: postgres
--

CREATE TABLE adtech.domain_app_changes_quarterly (
    domain_id integer,
    store_app integer,
    tag_source text,
    year smallint,
    quarter smallint,
    status text,
    batch_date date NOT NULL
)
PARTITION BY LIST (batch_date);


ALTER TABLE adtech.domain_app_changes_quarterly OWNER TO postgres;

--
-- Name: idx_domain_app_changes_lookup; Type: INDEX; Schema: adtech; Owner: postgres
--

CREATE INDEX idx_domain_app_changes_lookup ON ONLY adtech.domain_app_changes_quarterly USING btree (year, quarter, domain_id);


--
-- PostgreSQL database dump complete
--

\unrestrict raMiEzR70NRz7aWeSf1dn4nRc3ZOPuA6jsTFbKvf8RpHXWDDuXEc09WkhPhh1IF

