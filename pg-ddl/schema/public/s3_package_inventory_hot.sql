--
-- PostgreSQL database dump
--

\restrict BsZq6XMilnXg2gPpK7j8xr49OubzkqImdtx58daLZRWgXS2kOootfyEIDEoAl65

-- Dumped from database version 18.4 (Ubuntu 18.4-1.pgdg26.04+1)
-- Dumped by pg_dump version 18.4 (Ubuntu 18.4-1.pgdg26.04+1)

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
-- Name: s3_package_inventory_hot; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.s3_package_inventory_hot (
    myregion text NOT NULL,
    versionstr text NOT NULL,
    version_code_id integer,
    store_app integer,
    file_key text NOT NULL,
    last_modified timestamp with time zone NOT NULL,
    inserted_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.s3_package_inventory_hot OWNER TO postgres;

--
-- Name: s3_package_inventory_hot_myregion_file_key_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE UNIQUE INDEX s3_package_inventory_hot_myregion_file_key_idx ON public.s3_package_inventory_hot USING btree (myregion, file_key);


--
-- PostgreSQL database dump complete
--

\unrestrict BsZq6XMilnXg2gPpK7j8xr49OubzkqImdtx58daLZRWgXS2kOootfyEIDEoAl65

