--
-- PostgreSQL database dump
--

\restrict yKAuVMJzwJTq2GLXSNdKELpk7ooEZJVcQx2Ieb4oQVRmfPbSRSJ7Rf1b7DmlvO0

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
-- Name: sdk_strings; Type: MATERIALIZED VIEW; Schema: adtech; Owner: postgres
--

CREATE MATERIALIZED VIEW adtech.sdk_strings AS
 WITH matched_value_patterns AS (
         SELECT DISTINCT lower(vd.value_name) AS value_name_lower,
            sp.sdk_id
           FROM (public.version_strings vd
             JOIN adtech.sdk_packages sp ON ((lower(vd.value_name) ~~ (lower((sp.package_pattern)::text) || '%'::text))))
        ), matched_path_patterns AS (
         SELECT DISTINCT lower(vd.xml_path) AS xml_path_lower,
            ptm.sdk_id
           FROM (public.version_strings vd
             JOIN adtech.sdk_paths ptm ON ((lower(vd.xml_path) = lower((ptm.path_pattern)::text))))
        ), mediation_strings AS (
         SELECT vs.id AS version_string_id,
            cmp.sdk_id,
            lower(vs.value_name) AS value_name_lower
           FROM (public.version_strings vs
             JOIN adtech.sdk_mediation_patterns cmp ON ((lower(vs.value_name) ~~ (lower(concat((cmp.mediation_pattern)::text, '.')) || '%'::text))))
        )
 SELECT vs.id AS version_string_id,
    mp.sdk_id
   FROM (matched_value_patterns mp
     JOIN public.version_strings vs ON ((lower(vs.value_name) = mp.value_name_lower)))
UNION
 SELECT vs.id AS version_string_id,
    mp.sdk_id
   FROM (matched_path_patterns mp
     JOIN public.version_strings vs ON ((lower(vs.xml_path) = mp.xml_path_lower)))
UNION
 SELECT vs.id AS version_string_id,
    ms.sdk_id
   FROM (mediation_strings ms
     JOIN public.version_strings vs ON ((lower(vs.value_name) = ms.value_name_lower)))
  WITH NO DATA;


ALTER MATERIALIZED VIEW adtech.sdk_strings OWNER TO postgres;

--
-- Name: sdk_strings_version_string_id_sdk_id_idx; Type: INDEX; Schema: adtech; Owner: postgres
--

CREATE UNIQUE INDEX sdk_strings_version_string_id_sdk_id_idx ON adtech.sdk_strings USING btree (version_string_id, sdk_id);


--
-- PostgreSQL database dump complete
--

\unrestrict yKAuVMJzwJTq2GLXSNdKELpk7ooEZJVcQx2Ieb4oQVRmfPbSRSJ7Rf1b7DmlvO0

