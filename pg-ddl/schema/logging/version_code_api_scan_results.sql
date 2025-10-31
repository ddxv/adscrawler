--
-- PostgreSQL database dump
--

\restrict t34cWDOLbP2iOq680iY3fNiIu364ZbJyP0nE7IrbzTcPABDJKhwqxbcAjZ6N82p

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
-- Name: version_code_api_scan_results; Type: TABLE; Schema: logging; Owner: postgres
--

CREATE TABLE logging.version_code_api_scan_results (
    store_app bigint NOT NULL,
    version_code text,
    apk_hash text,
    crawl_result smallint,
    updated_at timestamp without time zone DEFAULT timezone('utc'::text, now()) NOT NULL
);


ALTER TABLE logging.version_code_api_scan_results OWNER TO postgres;

--
-- PostgreSQL database dump complete
--

\unrestrict t34cWDOLbP2iOq680iY3fNiIu364ZbJyP0nE7IrbzTcPABDJKhwqxbcAjZ6N82p

