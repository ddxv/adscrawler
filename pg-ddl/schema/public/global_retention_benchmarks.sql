--
-- PostgreSQL database dump
--

\restrict ZzbmOlxhPhLVUWDYI4sQ9mOAKsGLkJ3PEtJskHKE7wlaivPpClx0Cn2WGebJ9zH

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
-- Name: global_retention_benchmarks; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.global_retention_benchmarks (
    store_id smallint NOT NULL,
    app_category text NOT NULL,
    d1 numeric(6,5) NOT NULL,
    d7 numeric(6,5) NOT NULL,
    d30 numeric(6,5) NOT NULL,
    CONSTRAINT global_retention_benchmarks_d1_check CHECK (((d1 > (0)::numeric) AND (d1 <= (1)::numeric))),
    CONSTRAINT global_retention_benchmarks_d30_check CHECK (((d30 > (0)::numeric) AND (d30 <= (1)::numeric))),
    CONSTRAINT global_retention_benchmarks_d7_check CHECK (((d7 > (0)::numeric) AND (d7 <= (1)::numeric))),
    CONSTRAINT retention_monotonic_check CHECK (((d1 >= d7) AND (d7 >= d30)))
);


ALTER TABLE public.global_retention_benchmarks OWNER TO postgres;

--
-- Name: global_retention_benchmarks global_retention_benchmarks_pk; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.global_retention_benchmarks
    ADD CONSTRAINT global_retention_benchmarks_pk PRIMARY KEY (store_id, app_category);


--
-- PostgreSQL database dump complete
--

\unrestrict ZzbmOlxhPhLVUWDYI4sQ9mOAKsGLkJ3PEtJskHKE7wlaivPpClx0Cn2WGebJ9zH

