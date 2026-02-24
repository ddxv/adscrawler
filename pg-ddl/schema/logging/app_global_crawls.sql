--
-- PostgreSQL database dump
--

\restrict eElaEuOf87KrFvmTt8INvPS3UJYIFbIyaA4Gas2VDbd9wr2g41mk9UKbZgp2MYH

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
-- Name: app_global_crawls; Type: TABLE; Schema: logging; Owner: postgres
--

CREATE TABLE logging.app_global_crawls (
    crawl_result smallint,
    store_app integer,
    success_country_count smallint,
    crawled_at timestamp without time zone
);


ALTER TABLE logging.app_global_crawls OWNER TO postgres;

--
-- Name: idx_app_global_crawls_latest; Type: INDEX; Schema: logging; Owner: postgres
--

CREATE INDEX idx_app_global_crawls_latest ON logging.app_global_crawls USING btree (store_app, crawled_at DESC);


--
-- PostgreSQL database dump complete
--

\unrestrict eElaEuOf87KrFvmTt8INvPS3UJYIFbIyaA4Gas2VDbd9wr2g41mk9UKbZgp2MYH

