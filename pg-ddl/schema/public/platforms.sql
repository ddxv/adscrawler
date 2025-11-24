--
-- PostgreSQL database dump
--

\restrict 60Flx5LVy9uhf9bYtUcOqmdpOWbb4UMWpfSAMTprPJQbW3BBPi1eEnARJcADbcg

-- Dumped from database version 18.0 (Ubuntu 18.0-1.pgdg24.04+3)
-- Dumped by pg_dump version 18.0 (Ubuntu 18.0-1.pgdg24.04+3)

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
-- Name: platforms; Type: TABLE; Schema: public; Owner: james
--

CREATE TABLE public.platforms (
    id smallint NOT NULL,
    name character varying NOT NULL
);


ALTER TABLE public.platforms OWNER TO james;

--
-- Name: newtable_id_seq; Type: SEQUENCE; Schema: public; Owner: james
--

CREATE SEQUENCE public.newtable_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.newtable_id_seq OWNER TO james;

--
-- Name: newtable_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: james
--

ALTER SEQUENCE public.newtable_id_seq OWNED BY public.platforms.id;


--
-- Name: platforms id; Type: DEFAULT; Schema: public; Owner: james
--

ALTER TABLE ONLY public.platforms ALTER COLUMN id SET DEFAULT nextval('public.newtable_id_seq'::regclass);


--
-- Name: platforms platforms_pk; Type: CONSTRAINT; Schema: public; Owner: james
--

ALTER TABLE ONLY public.platforms
    ADD CONSTRAINT platforms_pk PRIMARY KEY (id);


--
-- Name: platforms platforms_un; Type: CONSTRAINT; Schema: public; Owner: james
--

ALTER TABLE ONLY public.platforms
    ADD CONSTRAINT platforms_un UNIQUE (name);


--
-- PostgreSQL database dump complete
--

\unrestrict 60Flx5LVy9uhf9bYtUcOqmdpOWbb4UMWpfSAMTprPJQbW3BBPi1eEnARJcADbcg

