--
-- PostgreSQL database dump
--

\restrict wyNUJb0Vwmb9ysTDYBGYiXZifu12TchyVWYCYnV5ho4GrYhLog7lcECkD0zgZ80

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
-- Name: api_call_urls; Type: TABLE; Schema: adtech; Owner: postgres
--

CREATE TABLE adtech.api_call_urls (
    id integer NOT NULL,
    run_id integer NOT NULL,
    api_call_id integer,
    url_id integer NOT NULL,
    created_at timestamp with time zone DEFAULT now()
);


ALTER TABLE adtech.api_call_urls OWNER TO postgres;

--
-- Name: api_call_urls_id_seq; Type: SEQUENCE; Schema: adtech; Owner: postgres
--

CREATE SEQUENCE adtech.api_call_urls_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE adtech.api_call_urls_id_seq OWNER TO postgres;

--
-- Name: api_call_urls_id_seq; Type: SEQUENCE OWNED BY; Schema: adtech; Owner: postgres
--

ALTER SEQUENCE adtech.api_call_urls_id_seq OWNED BY adtech.api_call_urls.id;


--
-- Name: api_call_urls id; Type: DEFAULT; Schema: adtech; Owner: postgres
--

ALTER TABLE ONLY adtech.api_call_urls ALTER COLUMN id SET DEFAULT nextval('adtech.api_call_urls_id_seq'::regclass);


--
-- Name: api_call_urls api_call_urls_pkey; Type: CONSTRAINT; Schema: adtech; Owner: postgres
--

ALTER TABLE ONLY adtech.api_call_urls
    ADD CONSTRAINT api_call_urls_pkey PRIMARY KEY (id);


--
-- Name: api_call_urls api_call_urls_run_id_api_call_id_url_id_key; Type: CONSTRAINT; Schema: adtech; Owner: postgres
--

ALTER TABLE ONLY adtech.api_call_urls
    ADD CONSTRAINT api_call_urls_run_id_api_call_id_url_id_key UNIQUE (run_id, api_call_id, url_id);


--
-- Name: idx_found_urls_run; Type: INDEX; Schema: adtech; Owner: postgres
--

CREATE INDEX idx_found_urls_run ON adtech.api_call_urls USING btree (run_id);


--
-- Name: api_call_urls api_call_urls_api_call_id_fkey; Type: FK CONSTRAINT; Schema: adtech; Owner: postgres
--

ALTER TABLE ONLY adtech.api_call_urls
    ADD CONSTRAINT api_call_urls_api_call_id_fkey FOREIGN KEY (api_call_id) REFERENCES public.api_calls(id);


--
-- Name: api_call_urls api_call_urls_run_id_fkey; Type: FK CONSTRAINT; Schema: adtech; Owner: postgres
--

ALTER TABLE ONLY adtech.api_call_urls
    ADD CONSTRAINT api_call_urls_run_id_fkey FOREIGN KEY (run_id) REFERENCES public.version_code_api_scan_results(id);


--
-- Name: api_call_urls api_call_urls_url_id_fkey; Type: FK CONSTRAINT; Schema: adtech; Owner: postgres
--

ALTER TABLE ONLY adtech.api_call_urls
    ADD CONSTRAINT api_call_urls_url_id_fkey FOREIGN KEY (url_id) REFERENCES adtech.urls(id);


--
-- PostgreSQL database dump complete
--

\unrestrict wyNUJb0Vwmb9ysTDYBGYiXZifu12TchyVWYCYnV5ho4GrYhLog7lcECkD0zgZ80

