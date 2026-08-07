--
-- PostgreSQL database dump
--

\restrict zlzHg9QPcHFTZu2MERdKooAWSpyAgJ5dcHObI8A3x8U861GytXZHaYgUAMAs2uf

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

SET default_table_access_method = heap;

--
-- Name: ad_formats; Type: TABLE; Schema: adtech; Owner: postgres
--

CREATE TABLE adtech.ad_formats (
    id smallint NOT NULL,
    name text NOT NULL
);


ALTER TABLE adtech.ad_formats OWNER TO postgres;

--
-- Name: ad_formats ad_formats_name_key; Type: CONSTRAINT; Schema: adtech; Owner: postgres
--

ALTER TABLE ONLY adtech.ad_formats
    ADD CONSTRAINT ad_formats_name_key UNIQUE (name);


--
-- Name: ad_formats ad_formats_pkey; Type: CONSTRAINT; Schema: adtech; Owner: postgres
--

ALTER TABLE ONLY adtech.ad_formats
    ADD CONSTRAINT ad_formats_pkey PRIMARY KEY (id);


--
-- PostgreSQL database dump complete
--

\unrestrict zlzHg9QPcHFTZu2MERdKooAWSpyAgJ5dcHObI8A3x8U861GytXZHaYgUAMAs2uf

