--
-- PostgreSQL database dump
--

\restrict NcNATM7s0UVvSyV7wPmXHRjHfufxRgmv6f24rC5pGOXCPG9dhopcnfFgIDZW5Tt

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
    five_star bigint
)
PARTITION BY RANGE (week_start);


ALTER TABLE public.app_global_metrics_history OWNER TO postgres;

--
-- Name: idx_agmh_store_app_week_start; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_agmh_store_app_week_start ON ONLY public.app_global_metrics_history USING btree (store_app, week_start);


--
-- PostgreSQL database dump complete
--

\unrestrict NcNATM7s0UVvSyV7wPmXHRjHfufxRgmv6f24rC5pGOXCPG9dhopcnfFgIDZW5Tt

