--
-- PostgreSQL database dump
--

\restrict aHsNA4USOiuv3YZqi5gs3i1lbaM6iMExHm0EhFWJPqwgCo5mUYN7pvPqnadw74t

-- Dumped from database version 17.6 (Ubuntu 17.6-2.pgdg24.04+1)
-- Dumped by pg_dump version 17.6 (Ubuntu 17.6-2.pgdg24.04+1)

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
-- Name: keywords_base; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.keywords_base (
    keyword_id bigint
);


ALTER TABLE public.keywords_base OWNER TO postgres;

--
-- PostgreSQL database dump complete
--

\unrestrict aHsNA4USOiuv3YZqi5gs3i1lbaM6iMExHm0EhFWJPqwgCo5mUYN7pvPqnadw74t
