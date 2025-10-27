--
-- PostgreSQL database dump
--

\restrict gtpYquhpzhRvm8Bx3SGESdBtKqj2niEb7XA71ZBq5QSLjgZBjEZrD1DWs6jCLH7

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
-- Name: company_categories; Type: MATERIALIZED VIEW; Schema: adtech; Owner: postgres
--

CREATE MATERIALIZED VIEW adtech.company_categories AS
SELECT DISTINCT
    c.id AS company_id,
    sc.category_id
FROM ((
    adtech.sdk_categories sc
    LEFT JOIN adtech.sdks AS sd ON ((sc.sdk_id = sd.id))
)
INNER JOIN adtech.companies AS c ON ((sd.company_id = c.id))
)
WITH NO DATA;


ALTER MATERIALIZED VIEW adtech.company_categories OWNER TO postgres;

--
-- PostgreSQL database dump complete
--

\unrestrict gtpYquhpzhRvm8Bx3SGESdBtKqj2niEb7XA71ZBq5QSLjgZBjEZrD1DWs6jCLH7
