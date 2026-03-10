--
-- PostgreSQL database dump
--

\restrict 4wF2e5OQkBynMWvZmFwVKdtLN5frOlpbgZWy3PJnu8MuIHYAKqhGstr37RLPtOb

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
    store_app integer CONSTRAINT new_app_global_metrics_weekly_store_app_not_null NOT NULL,
    week_start date CONSTRAINT new_app_global_metrics_weekly_week_start_not_null NOT NULL,
    weekly_installs bigint,
    weekly_ratings bigint,
    weekly_active_users bigint,
    monthly_active_users bigint,
    weekly_iap_revenue real,
    weekly_ad_revenue real,
    total_installs bigint,
    total_ratings bigint,
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
-- Name: app_global_metrics_history test_app_global_metrics_weekly_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.app_global_metrics_history
    ADD CONSTRAINT test_app_global_metrics_weekly_pkey PRIMARY KEY (store_app, week_start);


--
-- PostgreSQL database dump complete
--

\unrestrict 4wF2e5OQkBynMWvZmFwVKdtLN5frOlpbgZWy3PJnu8MuIHYAKqhGstr37RLPtOb

