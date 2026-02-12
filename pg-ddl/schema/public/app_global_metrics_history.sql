--
-- PostgreSQL database dump
--

\restrict 5zjPpDQfO089Bd3A7AeicDqFE0ePhx8QqJ0C0HHQ5A2cckJ16G8t1FMQch6fYGH

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
    store_last_updated date
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
-- Name: app_global_metrics_history_unique_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE UNIQUE INDEX app_global_metrics_history_unique_idx ON public.app_global_metrics_history USING btree (store_app, snapshot_date);


--
-- Name: app_global_metrics_history app_global_metrics_history_app_fk; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.app_global_metrics_history
    ADD CONSTRAINT app_global_metrics_history_app_fk FOREIGN KEY (store_app) REFERENCES public.store_apps(id);


--
-- PostgreSQL database dump complete
--

\unrestrict 5zjPpDQfO089Bd3A7AeicDqFE0ePhx8QqJ0C0HHQ5A2cckJ16G8t1FMQch6fYGH

