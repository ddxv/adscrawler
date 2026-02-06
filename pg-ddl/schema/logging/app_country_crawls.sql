--
-- PostgreSQL database dump
--

\restrict y2ATfZwO8k6eOpD6jYUuc9g2tfT4xh2hssXhQ5cWoau3PSwdPinDFZZNV2XMuVs

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
-- Name: app_country_crawls; Type: TABLE; Schema: logging; Owner: postgres
--

CREATE TABLE logging.app_country_crawls (
    crawl_result smallint,
    store_app integer,
    country_id smallint,
    crawled_at timestamp without time zone
);


ALTER TABLE logging.app_country_crawls OWNER TO postgres;

--
-- Name: app_country_crawls app_country_crawls_app_fk; Type: FK CONSTRAINT; Schema: logging; Owner: postgres
--

ALTER TABLE ONLY logging.app_country_crawls
    ADD CONSTRAINT app_country_crawls_app_fk FOREIGN KEY (store_app) REFERENCES public.store_apps(id);


--
-- Name: app_country_crawls fk_country; Type: FK CONSTRAINT; Schema: logging; Owner: postgres
--

ALTER TABLE ONLY logging.app_country_crawls
    ADD CONSTRAINT fk_country FOREIGN KEY (country_id) REFERENCES public.countries(id);


--
-- PostgreSQL database dump complete
--

\unrestrict y2ATfZwO8k6eOpD6jYUuc9g2tfT4xh2hssXhQ5cWoau3PSwdPinDFZZNV2XMuVs

