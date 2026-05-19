--
-- PostgreSQL database dump
--

\restrict u6id4aqMaaCrB9Yz62kCAiPDqZBRvLYMW06Mu9gap0UtOzL12UgBzZcs1ie0f4Y

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

\unrestrict u6id4aqMaaCrB9Yz62kCAiPDqZBRvLYMW06Mu9gap0UtOzL12UgBzZcs1ie0f4Y

