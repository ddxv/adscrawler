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
-- Name: app_keyword_rankings; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.app_keyword_rankings (
    id integer NOT NULL,
    crawled_date date NOT NULL,
    country smallint NOT NULL,
    lang smallint NOT NULL,
    keyword integer NOT NULL,
    rank smallint NOT NULL,
    store_app integer NOT NULL
);


ALTER TABLE public.app_keyword_rankings OWNER TO postgres;

--
-- Name: app_keyword_rankings_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.app_keyword_rankings_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.app_keyword_rankings_id_seq OWNER TO postgres;

--
-- Name: app_keyword_rankings_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.app_keyword_rankings_id_seq OWNED BY public.app_keyword_rankings.id;


--
-- Name: app_keyword_rankings id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.app_keyword_rankings ALTER COLUMN id SET DEFAULT nextval('public.app_keyword_rankings_id_seq'::regclass);


--
-- Name: app_keyword_rankings app_keyword_rankings_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.app_keyword_rankings
    ADD CONSTRAINT app_keyword_rankings_pkey PRIMARY KEY (id);


--
-- Name: app_keyword_rankings unique_keyword_ranking; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.app_keyword_rankings
    ADD CONSTRAINT unique_keyword_ranking UNIQUE (crawled_date, country, lang, rank, store_app, keyword);


--
-- Name: app_keyword_rankings fk_country; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.app_keyword_rankings
    ADD CONSTRAINT fk_country FOREIGN KEY (country) REFERENCES public.countries(id);


--
-- Name: app_keyword_rankings fk_language; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.app_keyword_rankings
    ADD CONSTRAINT fk_language FOREIGN KEY (lang) REFERENCES public.languages(id);


--
-- Name: app_keyword_rankings fk_store_app; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.app_keyword_rankings
    ADD CONSTRAINT fk_store_app FOREIGN KEY (store_app) REFERENCES public.store_apps(id);


--
-- Name: app_keyword_rankings fk_store_keyword; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.app_keyword_rankings
    ADD CONSTRAINT fk_store_keyword FOREIGN KEY (keyword) REFERENCES public.keywords(id);


--
-- PostgreSQL database dump complete
--

