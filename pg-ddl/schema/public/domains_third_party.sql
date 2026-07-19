--
-- PostgreSQL database dump
--

\restrict ni6Iea0c4rhf2bO0YHzqrwcShdkqWWqAa9WQHRIYeWrnQf4db8SUEaIyaET2xb3

-- Dumped from database version 18.3 (Ubuntu 18.3-1.pgdg24.04+1)
-- Dumped by pg_dump version 18.3 (Ubuntu 18.3-1.pgdg24.04+1)

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
-- Name: domains_third_party; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.domains_third_party (
    domain_id integer NOT NULL
);


ALTER TABLE public.domains_third_party OWNER TO postgres;

--
-- Name: domains_third_party domains_third_party_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.domains_third_party
    ADD CONSTRAINT domains_third_party_pkey PRIMARY KEY (domain_id);


--
-- Name: domains_third_party domains_third_party_domain_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.domains_third_party
    ADD CONSTRAINT domains_third_party_domain_id_fkey FOREIGN KEY (domain_id) REFERENCES public.domains(id);


--
-- PostgreSQL database dump complete
--

\unrestrict ni6Iea0c4rhf2bO0YHzqrwcShdkqWWqAa9WQHRIYeWrnQf4db8SUEaIyaET2xb3

