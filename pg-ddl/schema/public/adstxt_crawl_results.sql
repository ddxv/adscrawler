--
-- PostgreSQL database dump
--

\restrict WcPDJ3S8sZDvbjTw4h8uPN9kpoobW9Vn1alhKusoBkmPdvIkxbmFbeIEOrFcuSg

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
-- Name: adstxt_crawl_results; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.adstxt_crawl_results (
    id integer NOT NULL,
    domain_id integer NOT NULL,
    crawl_result integer,
    crawled_at timestamp without time zone,
    created_at timestamp without time zone DEFAULT timezone('utc'::text, now()),
    updated_at timestamp without time zone DEFAULT timezone('utc'::text, now())
);


ALTER TABLE public.adstxt_crawl_results OWNER TO postgres;

--
-- Name: adstxt_crawl_results_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.adstxt_crawl_results_id_seq
AS integer
START WITH 1
INCREMENT BY 1
NO MINVALUE
NO MAXVALUE
CACHE 1;


ALTER SEQUENCE public.adstxt_crawl_results_id_seq OWNER TO postgres;

--
-- Name: adstxt_crawl_results_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.adstxt_crawl_results_id_seq OWNED BY public.adstxt_crawl_results.id;


--
-- Name: adstxt_crawl_results id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.adstxt_crawl_results ALTER COLUMN id SET DEFAULT nextval(
    'public.adstxt_crawl_results_id_seq'::regclass
);


--
-- Name: adstxt_crawl_results adstxt_crawl_results_domain_un; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.adstxt_crawl_results
ADD CONSTRAINT adstxt_crawl_results_domain_un UNIQUE (domain_id);


--
-- Name: adstxt_crawl_results adstxt_crawl_results_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.adstxt_crawl_results
ADD CONSTRAINT adstxt_crawl_results_pkey PRIMARY KEY (id);


--
-- Name: adstxt_crawl_results adstxt_crawl_results_d_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.adstxt_crawl_results
ADD CONSTRAINT adstxt_crawl_results_d_fkey FOREIGN KEY (
    domain_id
) REFERENCES public.domains (id);


--
-- PostgreSQL database dump complete
--

\unrestrict WcPDJ3S8sZDvbjTw4h8uPN9kpoobW9Vn1alhKusoBkmPdvIkxbmFbeIEOrFcuSg
