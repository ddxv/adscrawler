--
-- PostgreSQL database dump
--

\restrict kNakK8OLmGbD6OPXWoUaKoX3jzMtqPD9nOFbLmlgWDI2BRJVpRt6NcTtOvaO4We

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
-- Name: urls; Type: TABLE; Schema: adtech; Owner: postgres
--

CREATE TABLE adtech.urls (
    id integer NOT NULL,
    url text NOT NULL,
    domain_id integer,
    scheme text NOT NULL,
    is_deep_link boolean GENERATED ALWAYS AS ((scheme <> ALL (ARRAY['http'::text, 'https'::text, 'ftp'::text]))) STORED,
    created_at timestamp with time zone DEFAULT now(),
    hostname text,
    url_hash character(32)
);


ALTER TABLE adtech.urls OWNER TO postgres;

--
-- Name: urls_id_seq; Type: SEQUENCE; Schema: adtech; Owner: postgres
--

CREATE SEQUENCE adtech.urls_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE adtech.urls_id_seq OWNER TO postgres;

--
-- Name: urls_id_seq; Type: SEQUENCE OWNED BY; Schema: adtech; Owner: postgres
--

ALTER SEQUENCE adtech.urls_id_seq OWNED BY adtech.urls.id;


--
-- Name: urls id; Type: DEFAULT; Schema: adtech; Owner: postgres
--

ALTER TABLE ONLY adtech.urls ALTER COLUMN id SET DEFAULT nextval('adtech.urls_id_seq'::regclass);


--
-- Name: urls urls_pkey; Type: CONSTRAINT; Schema: adtech; Owner: postgres
--

ALTER TABLE ONLY adtech.urls
    ADD CONSTRAINT urls_pkey PRIMARY KEY (id);


--
-- Name: idx_urls_domain_id; Type: INDEX; Schema: adtech; Owner: postgres
--

CREATE INDEX idx_urls_domain_id ON adtech.urls USING btree (domain_id);


--
-- Name: idx_urls_scheme; Type: INDEX; Schema: adtech; Owner: postgres
--

CREATE INDEX idx_urls_scheme ON adtech.urls USING btree (scheme);


--
-- Name: urls_url_hash_idx; Type: INDEX; Schema: adtech; Owner: postgres
--

CREATE UNIQUE INDEX urls_url_hash_idx ON adtech.urls USING btree (url_hash);


--
-- Name: urls urls_domain_id_fkey; Type: FK CONSTRAINT; Schema: adtech; Owner: postgres
--

ALTER TABLE ONLY adtech.urls
    ADD CONSTRAINT urls_domain_id_fkey FOREIGN KEY (domain_id) REFERENCES public.domains(id);


--
-- PostgreSQL database dump complete
--

\unrestrict kNakK8OLmGbD6OPXWoUaKoX3jzMtqPD9nOFbLmlgWDI2BRJVpRt6NcTtOvaO4We

