--
-- PostgreSQL database dump
--

\restrict ds3Vltal9T9UgJXyVlbqG0gTkFqL2MKzjEHBJMoaJfnMqAVoG2at8cxepzedzLD

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
-- Name: company_sdk_strings; Type: MATERIALIZED VIEW; Schema: adtech; Owner: postgres
--

CREATE MATERIALIZED VIEW adtech.company_sdk_strings AS
WITH matched_value_patterns AS (
    SELECT DISTINCT
        sd.company_id,
        lower(vd.value_name) AS value_name_lower
    FROM ((
        public.version_strings vd
        INNER JOIN
            adtech.sdk_packages AS sp
            ON
                (
                    (
                        lower(vd.value_name)
                        ~~ (lower((sp.package_pattern)::text) || '%'::text)
                    )
                )
    )
    INNER JOIN adtech.sdks AS sd ON ((sp.sdk_id = sd.id))
    )
), matched_path_patterns AS (
    SELECT DISTINCT
        sd.company_id,
        lower(vd.xml_path) AS xml_path_lower
    FROM ((
        public.version_strings vd
        INNER JOIN
            adtech.sdk_paths AS ptm
            ON ((lower(vd.xml_path) = lower((ptm.path_pattern)::text)))
    )
    INNER JOIN adtech.sdks AS sd ON ((ptm.sdk_id = sd.id))
    )
)
SELECT
    vs.id AS version_string_id,
    mp.company_id
FROM (
    matched_value_patterns AS mp
    INNER JOIN
        public.version_strings AS vs
        ON ((lower(vs.value_name) = mp.value_name_lower))
)
UNION
SELECT
    vs.id AS version_string_id,
    mp.company_id
FROM (
    matched_path_patterns AS mp
    INNER JOIN
        public.version_strings AS vs
        ON ((lower(vs.xml_path) = mp.xml_path_lower))
)
WITH NO DATA;


ALTER MATERIALIZED VIEW adtech.company_sdk_strings OWNER TO postgres;

--
-- Name: company_sdk_strings_version_string_id_company_id_idx; Type: INDEX; Schema: adtech; Owner: postgres
--

CREATE UNIQUE INDEX company_sdk_strings_version_string_id_company_id_idx ON adtech.company_sdk_strings USING btree (
    version_string_id, company_id
);


--
-- PostgreSQL database dump complete
--

\unrestrict ds3Vltal9T9UgJXyVlbqG0gTkFqL2MKzjEHBJMoaJfnMqAVoG2at8cxepzedzLD
