--
-- PostgreSQL database dump
--

\restrict P85TcTfaSwx6YnNK9kNt02X7lUUitE7zIXl4iZx1kJqOSKRX6V0IOE2pL9fMgth

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
-- Name: s3_processed_files; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.s3_processed_files (
    pipeline_name character varying(64) NOT NULL,
    file_path text NOT NULL,
    status character varying(16) NOT NULL,
    row_count bigint,
    error_message text,
    e_tag character varying(64),
    file_size_bytes bigint,
    processed_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT chk_s3_processed_files_status CHECK (((status)::text = ANY ((ARRAY['completed'::character varying, 'failed'::character varying])::text[])))
);


ALTER TABLE public.s3_processed_files OWNER TO postgres;

--
-- Name: s3_processed_files s3_processed_files_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.s3_processed_files
    ADD CONSTRAINT s3_processed_files_pkey PRIMARY KEY (pipeline_name, file_path);


--
-- PostgreSQL database dump complete
--

\unrestrict P85TcTfaSwx6YnNK9kNt02X7lUUitE7zIXl4iZx1kJqOSKRX6V0IOE2pL9fMgth

