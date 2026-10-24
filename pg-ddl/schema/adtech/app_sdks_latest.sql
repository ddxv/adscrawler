--
-- PostgreSQL database dump
--

\restrict 7XY1i0A6STEhP9rDlbCuCE2utH5OrIkfGPwceeeEWd1Xs2DsSkwnxld8I9UdfjZ

-- Dumped from database version 18.6 (Ubuntu 18.6-1.pgdg26.04+2)
-- Dumped by pg_dump version 18.6 (Ubuntu 18.6-1.pgdg26.04+2)

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

--
-- Name: app_sdks_latest; Type: TABLE; Schema: adtech; Owner: postgres
--

CREATE TABLE adtech.app_sdks_latest (
    store_app integer CONSTRAINT app_sdks_store_app_not_null NOT NULL,
    version_code_id integer CONSTRAINT app_sdks_version_code_id_not_null NOT NULL,
    sdk_id integer CONSTRAINT app_sdks_sdk_id_not_null NOT NULL,
    version_code_created_at timestamp without time zone CONSTRAINT app_sdks_version_code_created_at_not_null NOT NULL,
    batch_date date CONSTRAINT app_sdks_batch_date_not_null NOT NULL
)
PARTITION BY LIST (batch_date);


ALTER TABLE adtech.app_sdks_latest OWNER TO postgres;

--
-- PostgreSQL database dump complete
--

\unrestrict 7XY1i0A6STEhP9rDlbCuCE2utH5OrIkfGPwceeeEWd1Xs2DsSkwnxld8I9UdfjZ

