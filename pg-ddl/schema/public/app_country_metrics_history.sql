--
-- PostgreSQL database dump
--

\restrict mHoKXFdz8zLLs3LZXjh1yYMCxqdHehwakCJXygH9Yo24DKAeirHQWeGNf0zGYFf

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
-- Name: app_country_metrics_history; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.app_country_metrics_history (
    snapshot_date date NOT NULL,
    store_app integer NOT NULL,
    country_id smallint NOT NULL,
    review_count integer,
    rating real,
    rating_count integer,
    one_star integer,
    two_star integer,
    three_star integer,
    four_star integer,
    five_star integer,
    installs_est bigint
);


ALTER TABLE public.app_country_metrics_history OWNER TO postgres;

--
-- Name: app_country_metrics_history_date_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX app_country_metrics_history_date_idx ON public.app_country_metrics_history USING btree (snapshot_date);


--
-- Name: app_country_metrics_history_store_app_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX app_country_metrics_history_store_app_idx ON public.app_country_metrics_history USING btree (store_app);


--
-- PostgreSQL database dump complete
--

\unrestrict mHoKXFdz8zLLs3LZXjh1yYMCxqdHehwakCJXygH9Yo24DKAeirHQWeGNf0zGYFf

