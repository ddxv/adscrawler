--
-- PostgreSQL database dump
--

\restrict MOFoH4KlyHDTSKDluigaxnZKLeXxduqiaHa6lkrNp7yhMK9rLkMlHaw8LFaF2jr

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

\unrestrict MOFoH4KlyHDTSKDluigaxnZKLeXxduqiaHa6lkrNp7yhMK9rLkMlHaw8LFaF2jr

