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
-- Name: audit_dates; Type: MATERIALIZED VIEW; Schema: public; Owner: postgres
--

CREATE MATERIALIZED VIEW public.audit_dates AS
WITH sa AS (
    SELECT
        (store_apps_audit.stamp)::date AS updated_date,
        'store_apps'::text AS table_name,
        count(*) AS updated_count
    FROM logging.store_apps_audit
    GROUP BY ((store_apps_audit.stamp)::date)
)
SELECT
    updated_date,
    table_name,
    updated_count
FROM sa
WITH NO DATA;


ALTER MATERIALIZED VIEW public.audit_dates OWNER TO postgres;

--
-- Name: audit_dates_updated_date_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE UNIQUE INDEX audit_dates_updated_date_idx ON public.audit_dates USING btree (
    updated_date, table_name
);


--
-- PostgreSQL database dump complete
--
