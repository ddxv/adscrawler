--
-- PostgreSQL database dump
--

\restrict rLMM0usFF1IokGtUIBO8u2bgLfUEY2PmqIBfAweuCAVnbw5gsy0cEkOhC9UUfrG

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
-- Name: domain_app_changes_quarterly_20260719; Type: TABLE; Schema: adtech; Owner: postgres
--

CREATE TABLE adtech.domain_app_changes_quarterly_20260719 (
    domain_id integer,
    store_app integer,
    tag_source text,
    year smallint,
    quarter smallint,
    status text,
    batch_date date CONSTRAINT domain_app_changes_quarterly_batch_date_not_null NOT NULL,
    CONSTRAINT domain_app_changes_quarterly_20260719_batch_date_check CHECK ((batch_date = '2026-07-19'::date))
);


ALTER TABLE adtech.domain_app_changes_quarterly_20260719 OWNER TO postgres;

--
-- Name: domain_app_changes_quarterly_20260719; Type: TABLE ATTACH; Schema: adtech; Owner: postgres
--

ALTER TABLE ONLY adtech.domain_app_changes_quarterly ATTACH PARTITION adtech.domain_app_changes_quarterly_20260719 FOR VALUES IN ('2026-07-19');


--
-- Name: idx_domain_app_changes_quarterly_20260719_idx_domain_app_change; Type: INDEX; Schema: adtech; Owner: postgres
--

CREATE INDEX idx_domain_app_changes_quarterly_20260719_idx_domain_app_change ON adtech.domain_app_changes_quarterly_20260719 USING btree (year, quarter, domain_id);


--
-- Name: idx_domain_app_changes_quarterly_20260719_idx_domain_app_change; Type: INDEX ATTACH; Schema: adtech; Owner: postgres
--

ALTER INDEX adtech.idx_domain_app_changes_lookup ATTACH PARTITION adtech.idx_domain_app_changes_quarterly_20260719_idx_domain_app_change;


--
-- PostgreSQL database dump complete
--

\unrestrict rLMM0usFF1IokGtUIBO8u2bgLfUEY2PmqIBfAweuCAVnbw5gsy0cEkOhC9UUfrG

