--
-- PostgreSQL database dump
--

\restrict aAloXYXimhSSwkYGbutBesufaIk3UwvkBIAgiklkpbO3bGggNWll8XVKTdIENf9

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
-- Name: ecpm_benchmarks; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.ecpm_benchmarks (
    store smallint NOT NULL,
    tier_id smallint NOT NULL,
    ad_format_id smallint NOT NULL,
    ecpm numeric(6,2) NOT NULL
);


ALTER TABLE public.ecpm_benchmarks OWNER TO postgres;

--
-- Name: ecpm_benchmarks ad_revenue_benchmarks_pk; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.ecpm_benchmarks
    ADD CONSTRAINT ad_revenue_benchmarks_pk PRIMARY KEY (store, tier_id, ad_format_id);


--
-- Name: ecpm_benchmarks ecpm_benchmarks_ad_format_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.ecpm_benchmarks
    ADD CONSTRAINT ecpm_benchmarks_ad_format_id_fkey FOREIGN KEY (ad_format_id) REFERENCES adtech.ad_formats(id);


--
-- Name: ecpm_benchmarks ecpm_benchmarks_store_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.ecpm_benchmarks
    ADD CONSTRAINT ecpm_benchmarks_store_fkey FOREIGN KEY (store) REFERENCES public.stores(id);


--
-- Name: ecpm_benchmarks ecpm_benchmarks_tier_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.ecpm_benchmarks
    ADD CONSTRAINT ecpm_benchmarks_tier_id_fkey FOREIGN KEY (tier_id) REFERENCES public.tiers(id);


--
-- PostgreSQL database dump complete
--

\unrestrict aAloXYXimhSSwkYGbutBesufaIk3UwvkBIAgiklkpbO3bGggNWll8XVKTdIENf9

