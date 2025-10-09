--
-- PostgreSQL database dump
--

\restrict 8wgtZcxuHUu2Wzgsi2TqVVschBXQ8TtSLSy21Dj9HeKzHkCwq4V9FjG6zfyL45T

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
-- Name: ad_network_sdk_keys; Type: MATERIALIZED VIEW; Schema: public; Owner: postgres
--

CREATE MATERIALIZED VIEW public.ad_network_sdk_keys AS
WITH manifest_regex AS (
    SELECT
        vc.id AS version_code,
        vc.store_app,
        (
            regexp_match(
                vm.manifest_string,
                'applovin\.sdk\.key\"\ android\:value\=\"([^"]+)"'::text
            )
        )[1] AS applovin_sdk_key
    FROM (
        public.version_manifests AS vm
        LEFT JOIN public.version_codes AS vc ON ((vm.version_code = vc.id))
    )
), version_regex AS (
    SELECT
        vc.id AS version_code,
        vc.store_app,
        vs.value_name AS applovin_sdk_key
    FROM ((
        public.version_strings vs
        LEFT JOIN public.version_details_map AS vdm ON ((vs.id = vdm.string_id))
    )
    LEFT JOIN public.version_codes AS vc ON ((vdm.version_code = vc.id))
    )
    WHERE
        (
            (
                (vs.xml_path ~~* '%applovin%key%'::text)
                OR (vs.xml_path = 'applovin_settings.sdk_key'::text)
            )
            AND (length(vs.value_name) = 86)
        )
)
SELECT DISTINCT
    manifest_regex.store_app,
    manifest_regex.applovin_sdk_key
FROM manifest_regex
WHERE
    (
        (manifest_regex.applovin_sdk_key IS NOT null)
        AND (manifest_regex.applovin_sdk_key !~~ '@string%'::text)
    )
UNION
SELECT DISTINCT
    version_regex.store_app,
    version_regex.applovin_sdk_key
FROM version_regex
WITH NO DATA;


ALTER MATERIALIZED VIEW public.ad_network_sdk_keys OWNER TO postgres;

--
-- PostgreSQL database dump complete
--

\unrestrict 8wgtZcxuHUu2Wzgsi2TqVVschBXQ8TtSLSy21Dj9HeKzHkCwq4V9FjG6zfyL45T
