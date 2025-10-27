--
-- PostgreSQL database dump
--

\restrict JUYQHiSHTicNIzF0J0zgEtBlqc99RbTeCGa79FR1mbWp3M7yfrNwwFfXQjn5MUu

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
-- Name: click_url_redirect_chains; Type: TABLE; Schema: adtech; Owner: postgres
--

CREATE TABLE adtech.click_url_redirect_chains (
    id integer NOT NULL,
    run_id integer NOT NULL,
    url text NOT NULL,
    redirect_url text NOT NULL,
    api_call_id integer
);


ALTER TABLE adtech.click_url_redirect_chains OWNER TO postgres;

--
-- Name: click_url_redirect_chains_id_seq; Type: SEQUENCE; Schema: adtech; Owner: postgres
--

CREATE SEQUENCE adtech.click_url_redirect_chains_id_seq
AS integer
START WITH 1
INCREMENT BY 1
NO MINVALUE
NO MAXVALUE
CACHE 1;


ALTER SEQUENCE adtech.click_url_redirect_chains_id_seq OWNER TO postgres;

--
-- Name: click_url_redirect_chains_id_seq; Type: SEQUENCE OWNED BY; Schema: adtech; Owner: postgres
--

ALTER SEQUENCE adtech.click_url_redirect_chains_id_seq OWNED BY adtech.click_url_redirect_chains.id;


--
-- Name: click_url_redirect_chains id; Type: DEFAULT; Schema: adtech; Owner: postgres
--

ALTER TABLE ONLY adtech.click_url_redirect_chains ALTER COLUMN id SET DEFAULT nextval(
    'adtech.click_url_redirect_chains_id_seq'::regclass
);


--
-- Name: click_url_redirect_chains click_url_redirect_chains_pkey; Type: CONSTRAINT; Schema: adtech; Owner: postgres
--

ALTER TABLE ONLY adtech.click_url_redirect_chains
ADD CONSTRAINT click_url_redirect_chains_pkey PRIMARY KEY (id);


--
-- Name: click_url_redirect_chains_unique; Type: INDEX; Schema: adtech; Owner: postgres
--

CREATE UNIQUE INDEX click_url_redirect_chains_unique ON adtech.click_url_redirect_chains USING btree (
    run_id, md5(url), md5(redirect_url)
);


--
-- Name: click_url_redirect_chains fk_api_call_id; Type: FK CONSTRAINT; Schema: adtech; Owner: postgres
--

ALTER TABLE ONLY adtech.click_url_redirect_chains
ADD CONSTRAINT fk_api_call_id FOREIGN KEY (
    api_call_id
) REFERENCES public.api_calls (id);


--
-- Name: click_url_redirect_chains fk_run_id; Type: FK CONSTRAINT; Schema: adtech; Owner: postgres
--

ALTER TABLE ONLY adtech.click_url_redirect_chains
ADD CONSTRAINT fk_run_id FOREIGN KEY (
    run_id
) REFERENCES public.version_code_api_scan_results (id) ON DELETE CASCADE;


--
-- PostgreSQL database dump complete
--

\unrestrict JUYQHiSHTicNIzF0J0zgEtBlqc99RbTeCGa79FR1mbWp3M7yfrNwwFfXQjn5MUu
