--
-- PostgreSQL database dump
--

\restrict NplUThlmDggP2e0oeedSUeD5hD0gXvoa3DW9DkfzUOREUrtn9TM1kVWy7gAgxWE

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
-- Name: store_app_sdk_strings; Type: MATERIALIZED VIEW; Schema: adtech; Owner: postgres
--

CREATE MATERIALIZED VIEW adtech.store_app_sdk_strings AS
 WITH latest_version_codes AS (
         SELECT DISTINCT ON (vc_1.store_app) vc_1.id,
            vc_1.store_app,
            vc_1.version_code,
            vc_1.updated_at,
            vc_1.crawl_result
           FROM (public.version_codes vc_1
             JOIN public.version_code_sdk_scan_results vcssr ON ((vc_1.id = vcssr.version_code_id)))
          WHERE (vcssr.scan_result = 1)
          ORDER BY vc_1.store_app, (string_to_array((vc_1.version_code)::text, '.'::text))::bigint[] DESC
        )
 SELECT vc.store_app,
    vdm.string_id AS version_string_id,
    css.sdk_id
   FROM ((latest_version_codes vc
     JOIN public.version_details_map vdm ON ((vc.id = vdm.version_code)))
     LEFT JOIN adtech.sdk_strings css ON ((vdm.string_id = css.version_string_id)))
  WITH NO DATA;


ALTER MATERIALIZED VIEW adtech.store_app_sdk_strings OWNER TO postgres;

--
-- Name: store_app_sdk_strings_idx; Type: INDEX; Schema: adtech; Owner: postgres
--

CREATE UNIQUE INDEX store_app_sdk_strings_idx ON adtech.store_app_sdk_strings USING btree (store_app, version_string_id, sdk_id);


--
-- PostgreSQL database dump complete
--

\unrestrict NplUThlmDggP2e0oeedSUeD5hD0gXvoa3DW9DkfzUOREUrtn9TM1kVWy7gAgxWE

