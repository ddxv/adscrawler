--
-- PostgreSQL database dump
--

\restrict tcg02BkCjkhTIeXVQ2Ysuy5tcw1lRvBeDGjZk8ifgRJLrPyhnaGLSLa1Lihn7VF

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
-- Name: store_app_no_creatives; Type: TABLE; Schema: logging; Owner: postgres
--

CREATE TABLE logging.store_app_no_creatives (
    store_app_id bigint,
    pub_store_id text,
    run_id text
);


ALTER TABLE logging.store_app_no_creatives OWNER TO postgres;

--
-- PostgreSQL database dump complete
--

\unrestrict tcg02BkCjkhTIeXVQ2Ysuy5tcw1lRvBeDGjZk8ifgRJLrPyhnaGLSLa1Lihn7VF

