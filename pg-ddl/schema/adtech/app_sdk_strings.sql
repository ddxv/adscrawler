--
-- PostgreSQL database dump
--

\restrict yzLsxcyNPq5E7ZZRHFzMaXYgWIRBVNkg5CRPbcN7TcQ5iJRF0rJVdCEL2cotWY3

-- Dumped from database version 18.4 (Ubuntu 18.4-1.pgdg26.04+1)
-- Dumped by pg_dump version 18.4 (Ubuntu 18.4-1.pgdg26.04+1)

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
-- Name: app_sdk_strings; Type: TABLE; Schema: adtech; Owner: postgres
--

CREATE TABLE adtech.app_sdk_strings (
    store_app integer NOT NULL,
    string_id integer NOT NULL,
    sdk_id integer,
    batch_date date NOT NULL
)
PARTITION BY LIST (batch_date);


ALTER TABLE adtech.app_sdk_strings OWNER TO postgres;

--
-- PostgreSQL database dump complete
--

\unrestrict yzLsxcyNPq5E7ZZRHFzMaXYgWIRBVNkg5CRPbcN7TcQ5iJRF0rJVdCEL2cotWY3

