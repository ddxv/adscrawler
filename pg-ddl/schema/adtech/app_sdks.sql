--
-- PostgreSQL database dump
--

\restrict nSb7cF3vAWCM9NFjuENmGidgd3I1ZqjkjkcKja91m60c6YERsqTGcIq2yPKHOPK

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
-- Name: app_sdks; Type: TABLE; Schema: adtech; Owner: postgres
--

CREATE TABLE adtech.app_sdks (
    store_app integer NOT NULL,
    version_code_id integer NOT NULL,
    sdk_id integer NOT NULL,
    version_code_created_at timestamp without time zone NOT NULL,
    batch_date date NOT NULL
)
PARTITION BY LIST (batch_date);


ALTER TABLE adtech.app_sdks OWNER TO postgres;

--
-- PostgreSQL database dump complete
--

\unrestrict nSb7cF3vAWCM9NFjuENmGidgd3I1ZqjkjkcKja91m60c6YERsqTGcIq2yPKHOPK

