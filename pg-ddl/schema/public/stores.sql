--
-- PostgreSQL database dump
--

\restrict 8CJ2IctcwDiQ4G6iRhoJuImSD8GdPhNaURJe8ld7dln0lZSigLfIkaGMpD0IYJY

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
-- Name: stores; Type: TABLE; Schema: public; Owner: james
--

CREATE TABLE public.stores (
    id smallint NOT NULL,
    name character varying NOT NULL,
    platform smallint
);


ALTER TABLE public.stores OWNER TO james;

--
-- Name: stores_column1_seq; Type: SEQUENCE; Schema: public; Owner: james
--

CREATE SEQUENCE public.stores_column1_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.stores_column1_seq OWNER TO james;

--
-- Name: stores_column1_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: james
--

ALTER SEQUENCE public.stores_column1_seq OWNED BY public.stores.id;


--
-- Name: stores id; Type: DEFAULT; Schema: public; Owner: james
--

ALTER TABLE ONLY public.stores ALTER COLUMN id SET DEFAULT nextval('public.stores_column1_seq'::regclass);


--
-- Name: stores stores_pk; Type: CONSTRAINT; Schema: public; Owner: james
--

ALTER TABLE ONLY public.stores
    ADD CONSTRAINT stores_pk PRIMARY KEY (id);


--
-- Name: stores stores_un; Type: CONSTRAINT; Schema: public; Owner: james
--

ALTER TABLE ONLY public.stores
    ADD CONSTRAINT stores_un UNIQUE (name);


--
-- Name: stores stores_fk; Type: FK CONSTRAINT; Schema: public; Owner: james
--

ALTER TABLE ONLY public.stores
    ADD CONSTRAINT stores_fk FOREIGN KEY (platform) REFERENCES public.platforms(id);


--
-- PostgreSQL database dump complete
--

\unrestrict 8CJ2IctcwDiQ4G6iRhoJuImSD8GdPhNaURJe8ld7dln0lZSigLfIkaGMpD0IYJY

