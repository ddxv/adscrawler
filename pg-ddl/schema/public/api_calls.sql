--
-- PostgreSQL database dump
--

\restrict LD9SU1XNQEvT7SbWgdSDB9vyM5KxwhbQUayVWopQkHmsLXbo3w1kkb3kLYLmfPZ

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
-- Name: api_calls; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.api_calls (
    id integer NOT NULL,
    run_id integer NOT NULL,
    store_app integer NOT NULL,
    mitm_uuid uuid NOT NULL,
    flow_type text NOT NULL,
    tld_url text,
    status_code smallint NOT NULL,
    request_mime_type text,
    response_mime_type text,
    response_size_bytes integer,
    url text,
    ip_geo_snapshot_id integer,
    called_at timestamp without time zone NOT NULL,
    created_at timestamp with time zone DEFAULT timezone(
        'utc'::text, now()
    ) NOT NULL,
    updated_at timestamp with time zone DEFAULT timezone(
        'utc'::text, now()
    ) NOT NULL
);


ALTER TABLE public.api_calls OWNER TO postgres;

--
-- Name: api_calls_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.api_calls_id_seq
AS integer
START WITH 1
INCREMENT BY 1
NO MINVALUE
NO MAXVALUE
CACHE 1;


ALTER SEQUENCE public.api_calls_id_seq OWNER TO postgres;

--
-- Name: api_calls_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.api_calls_id_seq OWNED BY public.api_calls.id;


--
-- Name: api_calls id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.api_calls ALTER COLUMN id SET DEFAULT nextval(
    'public.api_calls_id_seq'::regclass
);


--
-- Name: api_calls api_calls_call_uuid_key; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.api_calls
ADD CONSTRAINT api_calls_call_uuid_key UNIQUE (mitm_uuid);


--
-- Name: api_calls api_calls_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.api_calls
ADD CONSTRAINT api_calls_pkey PRIMARY KEY (id);


--
-- Name: api_calls api_calls_ip_geo_snapshot_id_fk; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.api_calls
ADD CONSTRAINT api_calls_ip_geo_snapshot_id_fk FOREIGN KEY (
    ip_geo_snapshot_id
) REFERENCES public.ip_geo_snapshots (id);


--
-- Name: api_calls api_calls_run_fk; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.api_calls
ADD CONSTRAINT api_calls_run_fk FOREIGN KEY (
    run_id
) REFERENCES public.version_code_api_scan_results (id);


--
-- Name: api_calls api_calls_store_app_fk; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.api_calls
ADD CONSTRAINT api_calls_store_app_fk FOREIGN KEY (
    store_app
) REFERENCES public.store_apps (id) ON DELETE CASCADE;


--
-- PostgreSQL database dump complete
--

\unrestrict LD9SU1XNQEvT7SbWgdSDB9vyM5KxwhbQUayVWopQkHmsLXbo3w1kkb3kLYLmfPZ
