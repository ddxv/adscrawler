--
-- PostgreSQL database dump
--

\restrict 05ouKRxc4jXSbJspMU1EzOIaqFbx3JGkd1cQl409h7QvDQQn37fFVQPTyejf0m0

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
-- Name: app_global_metrics_weekly; Type: TABLE; Schema: logging; Owner: postgres
--

CREATE TABLE logging.app_global_metrics_weekly (
    store_app integer NOT NULL,
    updated_at timestamp without time zone DEFAULT timezone('utc'::text, now()) NOT NULL
);


ALTER TABLE logging.app_global_metrics_weekly OWNER TO postgres;

--
-- Name: app_global_metrics_weekly app_global_metrics_weekly_store_app_fkey; Type: FK CONSTRAINT; Schema: logging; Owner: postgres
--

ALTER TABLE ONLY logging.app_global_metrics_weekly
    ADD CONSTRAINT app_global_metrics_weekly_store_app_fkey FOREIGN KEY (store_app) REFERENCES public.store_apps(id);


--
-- PostgreSQL database dump complete
--

\unrestrict 05ouKRxc4jXSbJspMU1EzOIaqFbx3JGkd1cQl409h7QvDQQn37fFVQPTyejf0m0

