--
-- PostgreSQL database dump
--

\restrict 0nJwVxQWWcSoCvunNrJGFdXpOXhPTwwtJzUjFVO9KLmPLrA7NHZK8Q2HBTRXXab

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
-- Name: app_updated_history; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.app_updated_history (
    store_app integer NOT NULL,
    store_last_updated date NOT NULL,
    latest_crawl_week date NOT NULL
);


ALTER TABLE public.app_updated_history OWNER TO postgres;

--
-- Name: app_updated_history app_updated_history_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.app_updated_history
    ADD CONSTRAINT app_updated_history_pkey PRIMARY KEY (store_app, store_last_updated);


--
-- PostgreSQL database dump complete
--

\unrestrict 0nJwVxQWWcSoCvunNrJGFdXpOXhPTwwtJzUjFVO9KLmPLrA7NHZK8Q2HBTRXXab

