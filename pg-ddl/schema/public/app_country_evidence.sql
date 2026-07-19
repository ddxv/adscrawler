--
-- PostgreSQL database dump
--

\restrict YUvIkYTXJ3HgwqMIBQlN5xbvXch3z0MUzuLjfJlPXfhdtGz9qbSr1WYIEsnGiLn

-- Dumped from database version 18.3 (Ubuntu 18.3-1.pgdg24.04+1)
-- Dumped by pg_dump version 18.3 (Ubuntu 18.3-1.pgdg24.04+1)

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
-- Name: app_country_evidence; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.app_country_evidence (
    id bigint NOT NULL,
    store_app bigint NOT NULL,
    raw_address text NOT NULL,
    country_id smallint,
    updated_at timestamp with time zone DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC'::text)
);


ALTER TABLE public.app_country_evidence OWNER TO postgres;

--
-- Name: app_country_evidence_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

ALTER TABLE public.app_country_evidence ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.app_country_evidence_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: app_country_evidence store_app_country_evidence_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.app_country_evidence
    ADD CONSTRAINT store_app_country_evidence_pkey PRIMARY KEY (id);


--
-- Name: app_country_evidence unique_store_app_evidence; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.app_country_evidence
    ADD CONSTRAINT unique_store_app_evidence UNIQUE (store_app);


--
-- Name: app_country_evidence store_app_country_evidence_app_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.app_country_evidence
    ADD CONSTRAINT store_app_country_evidence_app_id_fkey FOREIGN KEY (store_app) REFERENCES public.store_apps(id) ON DELETE CASCADE;


--
-- Name: app_country_evidence store_app_country_evidence_country_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.app_country_evidence
    ADD CONSTRAINT store_app_country_evidence_country_id_fkey FOREIGN KEY (country_id) REFERENCES public.countries(id);


--
-- PostgreSQL database dump complete
--

\unrestrict YUvIkYTXJ3HgwqMIBQlN5xbvXch3z0MUzuLjfJlPXfhdtGz9qbSr1WYIEsnGiLn

