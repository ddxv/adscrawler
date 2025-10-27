--
-- PostgreSQL database dump
--

\restrict Z18swA26OC8GUUm2w2BtqmkDmEe41utUUjroNRaTJaQUJ61zZlBdTnN8O1JaONk

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
-- Name: companies_creative_rankings; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.companies_creative_rankings AS
WITH creative_rankings AS (
    SELECT
        ca.file_extension,
        ac_1.id AS api_call_id,
        cr.advertiser_store_app_id,
        cr.creative_initial_domain_id,
        cr.creative_host_domain_id,
        cr.additional_ad_domain_ids,
        vcasr.run_at,
        ca.md5_hash,
        COALESCE(ca.phash, ca.md5_hash) AS vhash
    FROM (((
        public.creative_records cr
        LEFT JOIN
            public.creative_assets AS ca
            ON ((cr.creative_asset_id = ca.id))
    )
    LEFT JOIN public.api_calls AS ac_1 ON ((cr.api_call_id = ac_1.id))
    )
    LEFT JOIN
        public.version_code_api_scan_results AS vcasr
        ON ((ac_1.run_id = vcasr.id))
    )
), combined_domains AS (
    SELECT
        cr.api_call_id,
        cr.vhash,
        cr.md5_hash,
        cr.file_extension,
        cr.creative_initial_domain_id AS domain_id,
        cr.advertiser_store_app_id,
        cr.run_at
    FROM creative_rankings AS cr
    UNION
    SELECT
        cr.api_call_id,
        cr.vhash,
        cr.md5_hash,
        cr.file_extension,
        cr.creative_host_domain_id,
        cr.advertiser_store_app_id,
        cr.run_at
    FROM creative_rankings AS cr
    UNION
    SELECT
        cr.api_call_id,
        cr.vhash,
        cr.md5_hash,
        cr.file_extension,
        UNNEST(cr.additional_ad_domain_ids) AS unnest,
        cr.advertiser_store_app_id,
        cr.run_at
    FROM creative_rankings AS cr
), visually_distinct AS (
    SELECT
        cdm.company_id,
        cd.file_extension,
        cd.advertiser_store_app_id,
        cd.vhash,
        MIN((cd.md5_hash)::text) AS md5_hash,
        MAX(cd.api_call_id) AS last_api_call_id,
        MAX(cd.run_at) AS last_seen
    FROM (
        combined_domains AS cd
        LEFT JOIN
            adtech.company_domain_mapping AS cdm
            ON ((cd.domain_id = cdm.domain_id))
    )
    GROUP BY
        cdm.company_id, cd.file_extension, cd.advertiser_store_app_id, cd.vhash
)
SELECT
    vd.company_id,
    vd.md5_hash,
    vd.file_extension,
    ad.domain_name AS company_domain,
    saa.name AS advertiser_name,
    saa.store,
    saa.store_id AS advertiser_store_id,
    sap.store_id AS publisher_store_id,
    sap.name AS publisher_name,
    saa.installs,
    saa.rating_count,
    saa.rating,
    saa.installs_sum_1w,
    saa.ratings_sum_1w,
    saa.installs_sum_4w,
    saa.ratings_sum_4w,
    vd.last_seen,
    CASE
        WHEN
            (saa.icon_url_100 IS NOT null)
            THEN
                (
                    CONCAT(
                        'https://media.appgoblin.info/app-icons/',
                        saa.store_id,
                        '/',
                        saa.icon_url_100
                    )
                )::character varying
        ELSE saa.icon_url_512
    END AS advertiser_icon_url,
    CASE
        WHEN
            (sap.icon_url_100 IS NOT null)
            THEN
                (
                    CONCAT(
                        'https://media.appgoblin.info/app-icons/',
                        sap.store_id,
                        '/',
                        sap.icon_url_100
                    )
                )::character varying
        ELSE sap.icon_url_512
    END AS publisher_icon_url
FROM (((((
    visually_distinct vd
    LEFT JOIN public.api_calls AS ac ON ((vd.last_api_call_id = ac.id))
)
LEFT JOIN adtech.companies AS c ON ((vd.company_id = c.id))
)
LEFT JOIN public.domains AS ad ON ((c.domain_id = ad.id))
)
LEFT JOIN
    frontend.store_apps_overview AS saa
    ON ((vd.advertiser_store_app_id = saa.id))
)
LEFT JOIN frontend.store_apps_overview AS sap ON ((ac.store_app = sap.id))
)
WHERE (c.id IS NOT null)
ORDER BY vd.last_seen DESC
WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.companies_creative_rankings OWNER TO postgres;

--
-- PostgreSQL database dump complete
--

\unrestrict Z18swA26OC8GUUm2w2BtqmkDmEe41utUUjroNRaTJaQUJ61zZlBdTnN8O1JaONk
