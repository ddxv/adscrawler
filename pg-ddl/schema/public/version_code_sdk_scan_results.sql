--
-- PostgreSQL database dump
--

\restrict jFV9pRBoi6LUlGtPgpNlPNmL3dkS4BpyGeWCVclisz29EET4sT39KhEV0I5LrJb

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
-- Name: version_code_sdk_scan_results; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.version_code_sdk_scan_results (
    id integer NOT NULL,
    version_code_id integer,
    scan_result smallint NOT NULL,
    scanned_at timestamp without time zone DEFAULT timezone('utc'::text, now()) NOT NULL
);


ALTER TABLE public.version_code_sdk_scan_results OWNER TO postgres;

--
-- Name: version_code_sdk_scan_results_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.version_code_sdk_scan_results_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.version_code_sdk_scan_results_id_seq OWNER TO postgres;

--
-- Name: version_code_sdk_scan_results_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.version_code_sdk_scan_results_id_seq OWNED BY public.version_code_sdk_scan_results.id;


--
-- Name: version_code_sdk_scan_results id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.version_code_sdk_scan_results ALTER COLUMN id SET DEFAULT nextval('public.version_code_sdk_scan_results_id_seq'::regclass);


--
-- Name: version_code_sdk_scan_results version_code_sdk_scan_results_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.version_code_sdk_scan_results
    ADD CONSTRAINT version_code_sdk_scan_results_pkey PRIMARY KEY (id);


--
-- Name: version_code_sdk_scan_results version_code_sdk_scan_results_version_code_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.version_code_sdk_scan_results
    ADD CONSTRAINT version_code_sdk_scan_results_version_code_id_fkey FOREIGN KEY (version_code_id) REFERENCES public.version_codes(id);


--
-- PostgreSQL database dump complete
--

\unrestrict jFV9pRBoi6LUlGtPgpNlPNmL3dkS4BpyGeWCVclisz29EET4sT39KhEV0I5LrJb

