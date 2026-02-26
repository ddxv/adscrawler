--
-- PostgreSQL database dump
--

\restrict 17shKy4dA1veZBvdg2dLdxOeC8Q9L0qpSwG27hczftbyHfcFiPlKv8LMrlTSesL

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
-- Name: crawl_scenarios; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.crawl_scenarios (
    id integer NOT NULL,
    name character varying(50) NOT NULL,
    description text,
    created_at timestamp with time zone DEFAULT now()
);


ALTER TABLE public.crawl_scenarios OWNER TO postgres;

--
-- Name: crawl_scenarios_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.crawl_scenarios_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.crawl_scenarios_id_seq OWNER TO postgres;

--
-- Name: crawl_scenarios_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.crawl_scenarios_id_seq OWNED BY public.crawl_scenarios.id;


--
-- Name: crawl_scenarios id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.crawl_scenarios ALTER COLUMN id SET DEFAULT nextval('public.crawl_scenarios_id_seq'::regclass);


--
-- Name: crawl_scenarios crawl_scenarios_name_key; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.crawl_scenarios
    ADD CONSTRAINT crawl_scenarios_name_key UNIQUE (name);


--
-- Name: crawl_scenarios crawl_scenarios_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.crawl_scenarios
    ADD CONSTRAINT crawl_scenarios_pkey PRIMARY KEY (id);


--
-- PostgreSQL database dump complete
--

\unrestrict 17shKy4dA1veZBvdg2dLdxOeC8Q9L0qpSwG27hczftbyHfcFiPlKv8LMrlTSesL

