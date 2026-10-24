--
-- PostgreSQL database dump
--

\restrict tY3C0korbEdL9ne3m8RKH8W413Woih7OF83FHaAHqq8q82EWdJByEcZ70Sp4VBe

-- Dumped from database version 18.6 (Ubuntu 18.6-1.pgdg26.04+2)
-- Dumped by pg_dump version 18.6 (Ubuntu 18.6-1.pgdg26.04+2)

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

--
-- Name: s3_package_inventory; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.s3_package_inventory (
    myregion text NOT NULL,
    versionstr text NOT NULL,
    version_code_id integer,
    store_app integer,
    file_key text NOT NULL,
    last_modified timestamp with time zone NOT NULL,
    batch_date date NOT NULL
)
PARTITION BY LIST (batch_date);


ALTER TABLE public.s3_package_inventory OWNER TO postgres;

--
-- Name: s3_package_inventory_myregion_file_key_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE UNIQUE INDEX s3_package_inventory_myregion_file_key_idx ON ONLY public.s3_package_inventory USING btree (myregion, file_key, batch_date);


--
-- PostgreSQL database dump complete
--

\unrestrict tY3C0korbEdL9ne3m8RKH8W413Woih7OF83FHaAHqq8q82EWdJByEcZ70Sp4VBe

