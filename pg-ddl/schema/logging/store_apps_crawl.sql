--
-- PostgreSQL database dump
--

\restrict QU8ePU9lODelbaExx1SzWmU4Z4xCyjnjGROQGgM0WgbWczu23iLN7rP3T86aLom

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
-- Name: store_apps_crawl; Type: TABLE; Schema: logging; Owner: postgres
--

CREATE TABLE logging.store_apps_crawl (
    index bigint,
    crawl_result bigint,
    store bigint,
    store_id text,
    store_app bigint,
    crawled_at timestamp without time zone
);


ALTER TABLE logging.store_apps_crawl OWNER TO postgres;

--
-- Name: ix_logging_store_apps_crawl_index; Type: INDEX; Schema: logging; Owner: postgres
--

CREATE INDEX ix_logging_store_apps_crawl_index ON logging.store_apps_crawl USING btree (
    index
);


--
-- PostgreSQL database dump complete
--

\unrestrict QU8ePU9lODelbaExx1SzWmU4Z4xCyjnjGROQGgM0WgbWczu23iLN7rP3T86aLom
