--
-- PostgreSQL database dump
--

\restrict abkOGGPRjyJVrWsnzSjMjGOZWDlegPKNwToHfg0gEn7ubHhf5v2OTvhp5HbvBE3

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
-- Name: keywords_base; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.keywords_base (
    keyword_id bigint
);


ALTER TABLE public.keywords_base OWNER TO postgres;

--
-- PostgreSQL database dump complete
--

\unrestrict abkOGGPRjyJVrWsnzSjMjGOZWDlegPKNwToHfg0gEn7ubHhf5v2OTvhp5HbvBE3
