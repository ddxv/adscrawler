--
-- PostgreSQL database dump
--

\restrict NkAYt5bAEwwhbb2kX3CGeCgj8Jl3L9Ti8bwQTw9blf2mdAoYBjkQYhiEgJ6wIDH

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
-- Name: store_app_z_scores_history; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.store_app_z_scores_history (
    target_week date NOT NULL,
    store character varying NOT NULL,
    store_app numeric NOT NULL,
    store_id character varying NOT NULL,
    app_name character varying NOT NULL,
    in_app_purchases boolean,
    ad_supported boolean,
    icon_url_100 text,
    icon_url_512 text,
    installs bigint,
    rating_count bigint,
    target_week_installs numeric,
    target_week_rating_count numeric,
    baseline_installs_2w numeric,
    baseline_ratings_2w numeric,
    installs_pct_increase numeric,
    ratings_pct_increase numeric,
    installs_z_score_1w numeric,
    ratings_z_score_1w numeric
);


ALTER TABLE public.store_app_z_scores_history OWNER TO postgres;

--
-- Name: store_app_z_scores_history store_app_z_scores_history_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.store_app_z_scores_history
    ADD CONSTRAINT store_app_z_scores_history_pkey PRIMARY KEY (target_week, store_id);


--
-- PostgreSQL database dump complete
--

\unrestrict NkAYt5bAEwwhbb2kX3CGeCgj8Jl3L9Ti8bwQTw9blf2mdAoYBjkQYhiEgJ6wIDH

