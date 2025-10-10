--
-- PostgreSQL database dump
--

\restrict EFZn82Mf4W0fGU79o72z6Tle2aRUQhq6S8SfslNQXUWcPTaJwGy8Yka0q061CGW

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

ALTER TABLE ONLY public.platforms ALTER COLUMN id SET DEFAULT nextval(
    'public.newtable_id_seq'::regclass
);


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

\unrestrict EFZn82Mf4W0fGU79o72z6Tle2aRUQhq6S8SfslNQXUWcPTaJwGy8Yka0q061CGW
