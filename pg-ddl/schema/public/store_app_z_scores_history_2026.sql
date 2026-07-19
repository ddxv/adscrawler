--
-- PostgreSQL database dump
--

\restrict 4pCElyI1hg5m32VEphNl95cVcKRL1ozdNYZphvIzzlcLGgfp2F7fNUDWfO2u4b1

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
-- Name: store_app_z_scores_history_2026; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.store_app_z_scores_history_2026 (
    target_week date,
    store bigint,
    store_app bigint,
    store_id text,
    app_name text,
    in_app_purchases boolean,
    ad_supported boolean,
    icon_url_100 text,
    target_week_installs bigint,
    baseline_installs double precision,
    baseline_installs_pct double precision,
    weekly_installs_pct double precision,
    installs_z_score_2w double precision,
    installs_z_score_4w double precision,
    installs_acceleration double precision,
    wow_growth_pct double precision,
    two_week_growth_pct double precision,
    momentum_pct double precision,
    has_reliable_baseline boolean
);


ALTER TABLE public.store_app_z_scores_history_2026 OWNER TO postgres;

--
-- PostgreSQL database dump complete
--

\unrestrict 4pCElyI1hg5m32VEphNl95cVcKRL1ozdNYZphvIzzlcLGgfp2F7fNUDWfO2u4b1

