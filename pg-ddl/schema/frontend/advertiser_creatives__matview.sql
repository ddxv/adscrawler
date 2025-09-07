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
-- Name: advertiser_creatives; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.advertiser_creatives AS
SELECT
    saa.store_id AS advertiser_store_id,
    cr.run_id,
    vcasr.run_at,
    sap.name AS pub_name,
    sap.store_id AS pub_store_id,
    hd.domain AS host_domain,
    hcd.domain AS host_domain_company_domain,
    hc.name AS host_domain_company_name,
    ad.domain AS ad_domain,
    acd.domain AS ad_domain_company_domain,
    ac.name AS ad_domain_company_name,
    ca.md5_hash,
    ca.file_extension,
    sap.icon_url_512 AS pub_icon_url_512,
    mmp.name AS mmp_name,
    mmpd.domain AS mmp_domain,
    cr.mmp_urls,
    COALESCE(ca.phash, ca.md5_hash) AS vhash,
    (
        SELECT ARRAY_AGG(ad_domains.domain) AS array_agg
        FROM public.ad_domains
        WHERE (ad_domains.id = ANY(cr.additional_ad_domain_ids))
    ) AS additional_ad_domain_urls
FROM (((((((((((((((
    public.creative_records cr
    LEFT JOIN public.creative_assets AS ca ON ((cr.creative_asset_id = ca.id))
)
LEFT JOIN
    frontend.store_apps_overview AS sap
    ON ((cr.store_app_pub_id = sap.id))
)
LEFT JOIN frontend.store_apps_overview AS saa ON ((ca.store_app_id = saa.id))
)
LEFT JOIN
    public.version_code_api_scan_results AS vcasr
    ON ((cr.run_id = vcasr.id))
)
LEFT JOIN public.ad_domains AS hd ON ((cr.creative_host_domain_id = hd.id))
)
LEFT JOIN public.ad_domains AS ad ON ((cr.creative_initial_domain_id = ad.id))
)
LEFT JOIN adtech.company_domain_mapping AS hcdm ON ((hd.id = hcdm.domain_id))
)
LEFT JOIN adtech.company_domain_mapping AS acdm ON ((hd.id = acdm.domain_id))
)
LEFT JOIN adtech.companies AS hc ON ((hcdm.company_id = hc.id))
)
LEFT JOIN adtech.companies AS ac ON ((acdm.company_id = ac.id))
)
LEFT JOIN public.ad_domains AS hcd ON ((hc.domain_id = hcd.id))
)
LEFT JOIN public.ad_domains AS acd ON ((ac.domain_id = acd.id))
)
LEFT JOIN
    adtech.company_domain_mapping AS cdm
    ON ((cr.mmp_domain_id = cdm.domain_id))
)
LEFT JOIN adtech.companies AS mmp ON ((cdm.company_id = mmp.id))
)
LEFT JOIN public.ad_domains AS mmpd ON ((cr.mmp_domain_id = mmpd.id))
)
WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.advertiser_creatives OWNER TO postgres;

--
-- PostgreSQL database dump complete
--
