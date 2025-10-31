--
-- PostgreSQL database dump
--

\restrict PpQDXVg0djtnfdVqcG5K3CnBgJgPhvr8sVbkIQdv5bOa8fZZwEzYIXthmACodVJ

-- Dumped from database version 18.0 (Ubuntu 18.0-1.pgdg24.04+3)
-- Dumped by pg_dump version 18.0 (Ubuntu 18.0-1.pgdg24.04+3)

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
    five_star integer
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
-- Name: app_country_metrics_history_unique_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE UNIQUE INDEX app_country_metrics_history_unique_idx ON public.app_country_metrics_history USING btree (store_app, country_id, snapshot_date);


--
-- Name: app_country_metrics_history app_country_metrics_history_app_fk; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.app_country_metrics_history
    ADD CONSTRAINT app_country_metrics_history_app_fk FOREIGN KEY (store_app) REFERENCES public.store_apps(id);


--
-- Name: app_country_metrics_history fk_country; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.app_country_metrics_history
    ADD CONSTRAINT fk_country FOREIGN KEY (country_id) REFERENCES public.countries(id);


--
-- PostgreSQL database dump complete
--

\unrestrict PpQDXVg0djtnfdVqcG5K3CnBgJgPhvr8sVbkIQdv5bOa8fZZwEzYIXthmACodVJ

