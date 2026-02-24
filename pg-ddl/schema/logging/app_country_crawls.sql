--
-- PostgreSQL database dump
--

\restrict KBg83bkBR3SHjpQpnsU8CMQuMndrmBVILRRH1kbXVqaM16NUQsCL9drbWMsXiC9

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
    crawl_result bigint,
    store_app bigint,
    country_id bigint,
    crawled_at timestamp with time zone
);


ALTER TABLE logging.app_country_crawls OWNER TO postgres;

--
-- Name: idx_app_country_crawls_latest; Type: INDEX; Schema: logging; Owner: postgres
--

CREATE INDEX idx_app_country_crawls_latest ON logging.app_country_crawls USING btree (store_app, crawled_at DESC);


--
-- PostgreSQL database dump complete
--

\unrestrict KBg83bkBR3SHjpQpnsU8CMQuMndrmBVILRRH1kbXVqaM16NUQsCL9drbWMsXiC9

