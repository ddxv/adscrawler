--
-- PostgreSQL database dump
--

\restrict XpK7VhuIXSSi9bhqGXBB39crt2wz32Y6ZCFTgzBXHbw8owln3Dxs2PUTbp2uplc

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
-- Name: company_value_string_mapping; Type: MATERIALIZED VIEW; Schema: adtech; Owner: postgres
--

CREATE MATERIALIZED VIEW adtech.company_value_string_mapping AS
SELECT
    vd.id AS version_string_id,
    sd.company_id,
    sp.sdk_id AS id
FROM ((
    public.version_strings vd
    INNER JOIN
        adtech.sdk_packages AS sp
        ON ((vd.value_name ~~* ((sp.package_pattern)::text || '%'::text)))
)
LEFT JOIN adtech.sdks AS sd ON ((sp.sdk_id = sd.id))
)
WITH NO DATA;


ALTER MATERIALIZED VIEW adtech.company_value_string_mapping OWNER TO postgres;

--
-- PostgreSQL database dump complete
--

\unrestrict XpK7VhuIXSSi9bhqGXBB39crt2wz32Y6ZCFTgzBXHbw8owln3Dxs2PUTbp2uplc
