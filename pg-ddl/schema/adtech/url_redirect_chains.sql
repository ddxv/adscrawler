--
-- PostgreSQL database dump
--

\restrict 9k8eFFsgEyhoRkq1Chf90pCbmczCV3kgqnUXdAYSHXgSajAeCZh057AtCbtkbHL

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
-- Name: url_redirect_chains; Type: TABLE; Schema: adtech; Owner: postgres
--

CREATE TABLE adtech.url_redirect_chains (
    id integer NOT NULL,
    run_id integer NOT NULL,
    api_call_id integer NOT NULL,
    url_id integer NOT NULL,
    next_url_id integer NOT NULL,
    hop_index integer NOT NULL,
    is_chain_start boolean DEFAULT false,
    is_chain_end boolean DEFAULT false,
    created_at timestamp with time zone DEFAULT now()
);


ALTER TABLE adtech.url_redirect_chains OWNER TO postgres;

--
-- Name: url_redirect_chains_id_seq; Type: SEQUENCE; Schema: adtech; Owner: postgres
--

CREATE SEQUENCE adtech.url_redirect_chains_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE adtech.url_redirect_chains_id_seq OWNER TO postgres;

--
-- Name: url_redirect_chains_id_seq; Type: SEQUENCE OWNED BY; Schema: adtech; Owner: postgres
--

ALTER SEQUENCE adtech.url_redirect_chains_id_seq OWNED BY adtech.url_redirect_chains.id;


--
-- Name: url_redirect_chains id; Type: DEFAULT; Schema: adtech; Owner: postgres
--

ALTER TABLE ONLY adtech.url_redirect_chains ALTER COLUMN id SET DEFAULT nextval('adtech.url_redirect_chains_id_seq'::regclass);


--
-- Name: url_redirect_chains url_redirect_chains_pkey; Type: CONSTRAINT; Schema: adtech; Owner: postgres
--

ALTER TABLE ONLY adtech.url_redirect_chains
    ADD CONSTRAINT url_redirect_chains_pkey PRIMARY KEY (id);


--
-- Name: url_redirect_chains_unique_idx; Type: INDEX; Schema: adtech; Owner: postgres
--

CREATE UNIQUE INDEX url_redirect_chains_unique_idx ON adtech.url_redirect_chains USING btree (run_id, api_call_id, url_id, next_url_id);


--
-- Name: url_redirect_chains url_redirect_chains_api_call_id_fkey; Type: FK CONSTRAINT; Schema: adtech; Owner: postgres
--

ALTER TABLE ONLY adtech.url_redirect_chains
    ADD CONSTRAINT url_redirect_chains_api_call_id_fkey FOREIGN KEY (api_call_id) REFERENCES public.api_calls(id);


--
-- Name: url_redirect_chains url_redirect_chains_next_url_id_fkey; Type: FK CONSTRAINT; Schema: adtech; Owner: postgres
--

ALTER TABLE ONLY adtech.url_redirect_chains
    ADD CONSTRAINT url_redirect_chains_next_url_id_fkey FOREIGN KEY (next_url_id) REFERENCES adtech.urls(id);


--
-- Name: url_redirect_chains url_redirect_chains_run_id_fkey; Type: FK CONSTRAINT; Schema: adtech; Owner: postgres
--

ALTER TABLE ONLY adtech.url_redirect_chains
    ADD CONSTRAINT url_redirect_chains_run_id_fkey FOREIGN KEY (run_id) REFERENCES public.version_code_api_scan_results(id) ON DELETE CASCADE;


--
-- Name: url_redirect_chains url_redirect_chains_url_id_fkey; Type: FK CONSTRAINT; Schema: adtech; Owner: postgres
--

ALTER TABLE ONLY adtech.url_redirect_chains
    ADD CONSTRAINT url_redirect_chains_url_id_fkey FOREIGN KEY (url_id) REFERENCES adtech.urls(id);


--
-- PostgreSQL database dump complete
--

\unrestrict 9k8eFFsgEyhoRkq1Chf90pCbmczCV3kgqnUXdAYSHXgSajAeCZh057AtCbtkbHL

