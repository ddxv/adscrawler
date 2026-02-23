--
-- PostgreSQL database dump
--

\restrict mr3AD6O7IfW9i75XyLyx5zd1DAiBQ5Yr65g1rbDUSYOBW9BkCidO3DW62kbOwBG

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
-- Name: retention_global_benchmarks; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.retention_global_benchmarks (
    store smallint CONSTRAINT global_retention_benchmarks_store_id_not_null NOT NULL,
    app_category text CONSTRAINT global_retention_benchmarks_app_category_not_null NOT NULL,
    d1 numeric(6,5) CONSTRAINT global_retention_benchmarks_d1_not_null NOT NULL,
    d7 numeric(6,5) CONSTRAINT global_retention_benchmarks_d7_not_null NOT NULL,
    d30 numeric(6,5) CONSTRAINT global_retention_benchmarks_d30_not_null NOT NULL,
    CONSTRAINT global_retention_benchmarks_d1_check CHECK (((d1 > (0)::numeric) AND (d1 <= (1)::numeric))),
    CONSTRAINT global_retention_benchmarks_d30_check CHECK (((d30 > (0)::numeric) AND (d30 <= (1)::numeric))),
    CONSTRAINT global_retention_benchmarks_d7_check CHECK (((d7 > (0)::numeric) AND (d7 <= (1)::numeric))),
    CONSTRAINT retention_monotonic_check CHECK (((d1 >= d7) AND (d7 >= d30)))
);


ALTER TABLE public.retention_global_benchmarks OWNER TO postgres;

--
-- Name: retention_global_benchmarks global_retention_benchmarks_pk; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.retention_global_benchmarks
    ADD CONSTRAINT global_retention_benchmarks_pk PRIMARY KEY (store, app_category);


--
-- PostgreSQL database dump complete
--

\unrestrict mr3AD6O7IfW9i75XyLyx5zd1DAiBQ5Yr65g1rbDUSYOBW9BkCidO3DW62kbOwBG

