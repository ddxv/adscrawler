--
-- PostgreSQL database dump
--

\restrict y3Oaj1ZbDncPwXy18ZBLHoIABnD3gjbE8ASt0dVx4QSp9FoUiRgOYZXcSLCGtKm

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
-- Name: app_country_crawls; Type: TABLE; Schema: logging; Owner: postgres
--

CREATE TABLE logging.app_country_crawls (
    crawl_result bigint NOT NULL,
    store_app bigint NOT NULL,
    country_id bigint NOT NULL,
    crawled_at timestamp with time zone NOT NULL
);


ALTER TABLE logging.app_country_crawls OWNER TO postgres;

--
-- Name: idx_app_country_crawls_latest; Type: INDEX; Schema: logging; Owner: postgres
--

CREATE INDEX idx_app_country_crawls_latest ON logging.app_country_crawls USING btree (store_app, crawled_at DESC);


--
-- PostgreSQL database dump complete
--

\unrestrict y3Oaj1ZbDncPwXy18ZBLHoIABnD3gjbE8ASt0dVx4QSp9FoUiRgOYZXcSLCGtKm

