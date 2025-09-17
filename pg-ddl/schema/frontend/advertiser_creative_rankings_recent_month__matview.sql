--
-- PostgreSQL database dump
--

-- Dumped from database version 17.5 (Ubuntu 17.5-1.pgdg24.04+1)
-- Dumped by pg_dump version 17.5 (Ubuntu 17.5-1.pgdg24.04+1)

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
-- Name: advertiser_creative_rankings_recent_month; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.advertiser_creative_rankings_recent_month AS
WITH creative_rankings AS (
    SELECT
        ca_1.md5_hash,
        ca_1.file_extension,
        ca_1.store_app_id AS advertiser_store_app_id,
        vcasr_1.run_at,
        row_number()
            OVER (PARTITION BY ca_1.store_app_id ORDER BY vcasr_1.run_at DESC)
            AS rn
    FROM ((
        public.creative_records cr_1
        LEFT JOIN
            public.creative_assets AS ca_1
            ON ((cr_1.creative_asset_id = ca_1.id))
    )
    LEFT JOIN
        public.version_code_api_scan_results AS vcasr_1
        ON ((cr_1.run_id = vcasr_1.id))
    )
)
SELECT
    saa.name AS advertiser_name,
    saa.store_id AS advertiser_store_id,
    saa.icon_url_100 AS advertiser_icon_url_100,
    saa.icon_url_512 AS advertiser_icon_url_512,
    count(DISTINCT ca.md5_hash) AS unique_creatives,
    count(DISTINCT cr.store_app_pub_id) AS unique_publishers,
    min(vcasr.run_at) AS first_seen,
    max(vcasr.run_at) AS last_seen,
    array_agg(DISTINCT ca.file_extension) AS file_types,
    avg(sap.installs) AS avg_publisher_installs,
    array(
        SELECT crk.md5_hash
        FROM creative_rankings AS crk
        WHERE ((crk.advertiser_store_app_id = saa.id) AND (crk.rn <= 5))
        ORDER BY crk.rn
    ) AS top_md5_hashes
FROM ((((
    public.creative_records cr
    LEFT JOIN public.creative_assets AS ca ON ((cr.creative_asset_id = ca.id))
)
LEFT JOIN
    frontend.store_apps_overview AS sap
    ON ((cr.store_app_pub_id = sap.id))
)
LEFT JOIN public.store_apps AS saa ON ((ca.store_app_id = saa.id))
)
LEFT JOIN
    public.version_code_api_scan_results AS vcasr
    ON ((cr.run_id = vcasr.id))
)
WHERE (vcasr.run_at >= (now() - '1 mon'::interval))
GROUP BY saa.name, saa.store_id, saa.icon_url_512, saa.id
ORDER BY (count(DISTINCT cr.store_app_pub_id)) DESC
WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.advertiser_creative_rankings_recent_month OWNER TO postgres;

--
-- PostgreSQL database dump complete
--
