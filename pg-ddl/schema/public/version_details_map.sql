--
-- PostgreSQL database dump
--

\restrict J5ZW9es2KSacdqYkl1R8VN1iflxmz4JMyEjz4eBheleCHOPeMZsotAxrHDlPi2Q

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
-- Name: version_details_map; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.version_details_map (
    version_code bigint NOT NULL,
    string_id integer NOT NULL
);


ALTER TABLE public.version_details_map OWNER TO postgres;

--
-- Name: version_details_map version_details_map_unique; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.version_details_map
    ADD CONSTRAINT version_details_map_unique UNIQUE (version_code, string_id);


--
-- Name: version_details_map version_details_map_string_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.version_details_map
    ADD CONSTRAINT version_details_map_string_id_fkey FOREIGN KEY (string_id) REFERENCES public.version_strings(id);


--
-- PostgreSQL database dump complete
--

\unrestrict J5ZW9es2KSacdqYkl1R8VN1iflxmz4JMyEjz4eBheleCHOPeMZsotAxrHDlPi2Q

