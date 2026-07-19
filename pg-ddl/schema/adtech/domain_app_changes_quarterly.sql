--
-- PostgreSQL database dump
--

\restrict 9iBV5Lep42DuoU96KqRnrHFPnExNmGN48V2dIKc8qkXQvo7ZPT6ZRoYhzFhGZ00

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

\unrestrict 9iBV5Lep42DuoU96KqRnrHFPnExNmGN48V2dIKc8qkXQvo7ZPT6ZRoYhzFhGZ00

