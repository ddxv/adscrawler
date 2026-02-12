--
-- PostgreSQL database dump
--

\restrict bw6tZX2Eg9497YQpaxF7byAcdVq4sFuOP6eJgv61OGh7ReakJRoEKZlB768p1Zr

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
-- Name: sdk_categories; Type: TABLE; Schema: adtech; Owner: postgres
--

CREATE TABLE adtech.sdk_categories (
    sdk_id integer NOT NULL,
    category_id integer NOT NULL
);


ALTER TABLE adtech.sdk_categories OWNER TO postgres;

--
-- Name: sdk_categories sdk_categories_pkey; Type: CONSTRAINT; Schema: adtech; Owner: postgres
--

ALTER TABLE ONLY adtech.sdk_categories
    ADD CONSTRAINT sdk_categories_pkey PRIMARY KEY (sdk_id, category_id);


--
-- Name: sdk_categories fk_category; Type: FK CONSTRAINT; Schema: adtech; Owner: postgres
--

ALTER TABLE ONLY adtech.sdk_categories
    ADD CONSTRAINT fk_category FOREIGN KEY (category_id) REFERENCES adtech.categories(id) ON DELETE CASCADE;


--
-- Name: sdk_categories fk_sdk; Type: FK CONSTRAINT; Schema: adtech; Owner: postgres
--

ALTER TABLE ONLY adtech.sdk_categories
    ADD CONSTRAINT fk_sdk FOREIGN KEY (sdk_id) REFERENCES adtech.sdks(id) ON DELETE CASCADE;


--
-- PostgreSQL database dump complete
--

\unrestrict bw6tZX2Eg9497YQpaxF7byAcdVq4sFuOP6eJgv61OGh7ReakJRoEKZlB768p1Zr

