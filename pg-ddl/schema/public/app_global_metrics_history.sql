--
-- PostgreSQL database dump
--

\restrict OnvaLLOco1qPm7fjAxHYJWQ0dLXKjAH3g2zDQ1RVjQ46wZ6MrjjQWkr7arGFOBv

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
-- Name: app_global_metrics_history; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.app_global_metrics_history (
    snapshot_date date NOT NULL,
    store_app integer NOT NULL,
    installs bigint,
    rating_count bigint,
    review_count bigint,
    rating real,
    one_star bigint,
    two_star bigint,
    three_star bigint,
    four_star bigint,
    five_star bigint,
    store_last_updated date,
    tier1_pct smallint,
    tier2_pct smallint,
    tier3_pct smallint
);


ALTER TABLE public.app_global_metrics_history OWNER TO postgres;

--
-- Name: app_global_metrics_history_date_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX app_global_metrics_history_date_idx ON public.app_global_metrics_history USING btree (snapshot_date);


--
-- Name: app_global_metrics_history_store_app_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX app_global_metrics_history_store_app_idx ON public.app_global_metrics_history USING btree (store_app);


--
-- PostgreSQL database dump complete
--

\unrestrict OnvaLLOco1qPm7fjAxHYJWQ0dLXKjAH3g2zDQ1RVjQ46wZ6MrjjQWkr7arGFOBv

