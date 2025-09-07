--
-- PostgreSQL database dump
--

-- Dumped from database version 17.5 (Ubuntu 17.5-1.pgdg24.04+1)
-- Dumped by pg_dump version 17.5 (Ubuntu 17.5-1.pgdg24.04+1)

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
-- Name: store_app_api_calls; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.store_app_api_calls (
    id integer NOT NULL,
    store_app integer NOT NULL,
    tld_url text NOT NULL,
    url text NOT NULL,
    host text NOT NULL,
    status_code integer NOT NULL,
    called_at timestamp without time zone NOT NULL,
    run_id integer NOT NULL,
    country_id integer,
    state_iso character varying(4),
    city_name character varying,
    org character varying
);


ALTER TABLE public.store_app_api_calls OWNER TO postgres;

--
-- Name: store_app_api_calls_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.store_app_api_calls_id_seq
AS integer
START WITH 1
INCREMENT BY 1
NO MINVALUE
NO MAXVALUE
CACHE 1;


ALTER SEQUENCE public.store_app_api_calls_id_seq OWNER TO postgres;

--
-- Name: store_app_api_calls_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.store_app_api_calls_id_seq OWNED BY public.store_app_api_calls.id;


--
-- Name: store_app_api_calls id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.store_app_api_calls ALTER COLUMN id SET DEFAULT nextval(
    'public.store_app_api_calls_id_seq'::regclass
);


--
-- Name: store_app_api_calls store_app_api_calls_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.store_app_api_calls
ADD CONSTRAINT store_app_api_calls_pkey PRIMARY KEY (id);


--
-- Name: store_app_api_calls unique_store_app_api_calls; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.store_app_api_calls
ADD CONSTRAINT unique_store_app_api_calls UNIQUE (
    store_app, tld_url, url, host, status_code, called_at
);


--
-- Name: store_app_api_calls fk_country; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.store_app_api_calls
ADD CONSTRAINT fk_country FOREIGN KEY (
    country_id
) REFERENCES public.countries (id);


--
-- Name: store_app_api_calls store_app_api_call_fk; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.store_app_api_calls
ADD CONSTRAINT store_app_api_call_fk FOREIGN KEY (
    store_app
) REFERENCES public.store_apps (id) ON DELETE CASCADE;


--
-- Name: store_app_api_calls store_app_api_calls_api_scan_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.store_app_api_calls
ADD CONSTRAINT store_app_api_calls_api_scan_id_fkey FOREIGN KEY (
    run_id
) REFERENCES public.version_code_api_scan_results (id);


--
-- PostgreSQL database dump complete
--
