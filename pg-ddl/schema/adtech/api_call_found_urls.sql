--
-- PostgreSQL database dump
--

\restrict 4lxco5pb3b48FzH6zwmQ9LhFLqiwqAjTLWGEwrIJKX9B7penRxqSDdCEJOTEa1i

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
-- Name: api_call_found_urls; Type: TABLE; Schema: adtech; Owner: postgres
--

CREATE TABLE adtech.api_call_found_urls (
    id integer NOT NULL,
    run_id integer NOT NULL,
    api_call_id integer,
    url text NOT NULL
);


ALTER TABLE adtech.api_call_found_urls OWNER TO postgres;

--
-- Name: api_call_found_urls_id_seq; Type: SEQUENCE; Schema: adtech; Owner: postgres
--

CREATE SEQUENCE adtech.api_call_found_urls_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE adtech.api_call_found_urls_id_seq OWNER TO postgres;

--
-- Name: api_call_found_urls_id_seq; Type: SEQUENCE OWNED BY; Schema: adtech; Owner: postgres
--

ALTER SEQUENCE adtech.api_call_found_urls_id_seq OWNED BY adtech.api_call_found_urls.id;


--
-- Name: api_call_found_urls id; Type: DEFAULT; Schema: adtech; Owner: postgres
--

ALTER TABLE ONLY adtech.api_call_found_urls ALTER COLUMN id SET DEFAULT nextval('adtech.api_call_found_urls_id_seq'::regclass);


--
-- Name: api_call_found_urls found_urls_pkey; Type: CONSTRAINT; Schema: adtech; Owner: postgres
--

ALTER TABLE ONLY adtech.api_call_found_urls
    ADD CONSTRAINT found_urls_pkey PRIMARY KEY (id);


--
-- Name: found_urls_idx; Type: INDEX; Schema: adtech; Owner: postgres
--

CREATE UNIQUE INDEX found_urls_idx ON adtech.api_call_found_urls USING btree (run_id, api_call_id, md5(url));


--
-- Name: api_call_found_urls fk_api_call_id; Type: FK CONSTRAINT; Schema: adtech; Owner: postgres
--

ALTER TABLE ONLY adtech.api_call_found_urls
    ADD CONSTRAINT fk_api_call_id FOREIGN KEY (api_call_id) REFERENCES public.api_calls(id);


--
-- Name: api_call_found_urls fk_run_id; Type: FK CONSTRAINT; Schema: adtech; Owner: postgres
--

ALTER TABLE ONLY adtech.api_call_found_urls
    ADD CONSTRAINT fk_run_id FOREIGN KEY (run_id) REFERENCES public.version_code_api_scan_results(id);


--
-- PostgreSQL database dump complete
--

\unrestrict 4lxco5pb3b48FzH6zwmQ9LhFLqiwqAjTLWGEwrIJKX9B7penRxqSDdCEJOTEa1i

