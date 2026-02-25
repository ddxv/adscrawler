--
-- PostgreSQL database dump
--

\restrict Tfac6mLNbZ3AvQyWOzQrS0eu2JaeRotUG6h4bFhBFpkF7kdN3kcR1C9TzApa4bE

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
-- Name: store_apps_descriptions; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.store_apps_descriptions (
    id integer NOT NULL,
    store_app integer NOT NULL,
    language_id integer NOT NULL,
    description text NOT NULL,
    description_short text NOT NULL,
    title text,
    updated_at timestamp without time zone DEFAULT now() NOT NULL,
    description_tsv tsvector
);


ALTER TABLE public.store_apps_descriptions OWNER TO postgres;

--
-- Name: store_apps_descriptions_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.store_apps_descriptions_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.store_apps_descriptions_id_seq OWNER TO postgres;

--
-- Name: store_apps_descriptions_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.store_apps_descriptions_id_seq OWNED BY public.store_apps_descriptions.id;


--
-- Name: store_apps_descriptions id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.store_apps_descriptions ALTER COLUMN id SET DEFAULT nextval('public.store_apps_descriptions_id_seq'::regclass);


--
-- Name: store_apps_descriptions store_apps_descriptions_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.store_apps_descriptions
    ADD CONSTRAINT store_apps_descriptions_pkey PRIMARY KEY (id);


--
-- Name: idx_description_tsv; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_description_tsv ON public.store_apps_descriptions USING gin (description_tsv);


--
-- Name: store_apps_descriptions_unique_hash_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE UNIQUE INDEX store_apps_descriptions_unique_hash_idx ON public.store_apps_descriptions USING btree (store_app, language_id, md5(description), md5(description_short));


--
-- Name: store_apps_descriptions store_apps_descriptions_language_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.store_apps_descriptions
    ADD CONSTRAINT store_apps_descriptions_language_id_fkey FOREIGN KEY (language_id) REFERENCES public.languages(id) ON DELETE CASCADE;


--
-- Name: store_apps_descriptions store_apps_descriptions_store_app_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.store_apps_descriptions
    ADD CONSTRAINT store_apps_descriptions_store_app_fkey FOREIGN KEY (store_app) REFERENCES public.store_apps(id) ON DELETE CASCADE;


--
-- PostgreSQL database dump complete
--

\unrestrict Tfac6mLNbZ3AvQyWOzQrS0eu2JaeRotUG6h4bFhBFpkF7kdN3kcR1C9TzApa4bE

