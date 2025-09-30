--
-- PostgreSQL database dump
--

\restrict 0j8bdSkxsgjFpIKVv9pJVOz3dmfWqo3gyjR8C6bOgNIOLs3cTsxeBccP87IvDcp

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
-- Name: combined_store_apps_companies; Type: MATERIALIZED VIEW; Schema: adtech; Owner: postgres
--

CREATE MATERIALIZED VIEW adtech.combined_store_apps_companies AS
WITH api_based_companies AS (
    SELECT DISTINCT
        saac.store_app,
        cm.mapped_category AS app_category,
        cdm.company_id,
        c_1.parent_company_id AS parent_id,
        'api_call'::text AS tag_source,
        COALESCE(cad_1.domain, (saac.tld_url)::character varying) AS ad_domain
    FROM ((((((
        public.store_app_api_calls saac
        LEFT JOIN public.store_apps AS sa_1 ON ((saac.store_app = sa_1.id))
    )
    LEFT JOIN
        public.category_mapping AS cm
        ON (((sa_1.category)::text = (cm.original_category)::text))
    )
    LEFT JOIN
        public.ad_domains AS ad_1
        ON ((saac.tld_url = (ad_1.domain)::text))
    )
    LEFT JOIN
        adtech.company_domain_mapping AS cdm
        ON ((ad_1.id = cdm.domain_id))
    )
    LEFT JOIN adtech.companies AS c_1 ON ((cdm.company_id = c_1.id))
    )
    LEFT JOIN public.ad_domains AS cad_1 ON ((c_1.domain_id = cad_1.id))
    )
), sdk_based_companies AS (
    SELECT
        sac.store_app,
        cm.mapped_category AS app_category,
        sac.company_id,
        sac.parent_id,
        ad_1.domain AS ad_domain,
        'sdk'::text AS tag_source
    FROM ((((
        adtech.store_apps_companies_sdk sac
        LEFT JOIN adtech.companies AS c_1 ON ((sac.company_id = c_1.id))
    )
    LEFT JOIN public.ad_domains AS ad_1 ON ((c_1.domain_id = ad_1.id))
    )
    LEFT JOIN public.store_apps AS sa_1 ON ((sac.store_app = sa_1.id))
    )
    LEFT JOIN
        public.category_mapping AS cm
        ON (((sa_1.category)::text = (cm.original_category)::text))
    )
), distinct_ad_and_pub_domains AS (
    SELECT DISTINCT
        pd.url AS publisher_domain_url,
        ad_1.domain AS ad_domain_url,
        aae.relationship
    FROM (((
        public.app_ads_entrys aae
        LEFT JOIN public.ad_domains AS ad_1 ON ((aae.ad_domain = ad_1.id))
    )
    LEFT JOIN public.app_ads_map AS aam ON ((aae.id = aam.app_ads_entry))
    )
    LEFT JOIN public.pub_domains AS pd ON ((aam.pub_domain = pd.id))
    )
    WHERE ((pd.crawled_at - aam.updated_at) < '01:00:00'::interval)
), adstxt_based_companies AS (
    SELECT DISTINCT
        aum.store_app,
        cm.mapped_category AS app_category,
        c_1.id AS company_id,
        pnv.ad_domain_url AS ad_domain,
        COALESCE(c_1.parent_company_id, c_1.id) AS parent_id,
        CASE
            WHEN
                ((pnv.relationship)::text = 'DIRECT'::text)
                THEN 'app_ads_direct'::text
            WHEN
                ((pnv.relationship)::text = 'RESELLER'::text)
                THEN 'app_ads_reseller'::text
            ELSE 'app_ads_unknown'::text
        END AS tag_source
    FROM ((((((
        public.app_urls_map aum
        LEFT JOIN public.pub_domains AS pd ON ((aum.pub_domain = pd.id))
    )
    LEFT JOIN
        distinct_ad_and_pub_domains AS pnv
        ON (((pd.url)::text = (pnv.publisher_domain_url)::text))
    )
    LEFT JOIN
        public.ad_domains AS ad_1
        ON (((pnv.ad_domain_url)::text = (ad_1.domain)::text))
    )
    LEFT JOIN adtech.companies AS c_1 ON ((ad_1.id = c_1.domain_id))
    )
    LEFT JOIN public.store_apps AS sa_1 ON ((aum.store_app = sa_1.id))
    )
    LEFT JOIN
        public.category_mapping AS cm
        ON (((sa_1.category)::text = (cm.original_category)::text))
    )
    WHERE
        (
            (sa_1.crawl_result = 1)
            AND ((pnv.ad_domain_url IS NOT null) OR (c_1.id IS NOT null))
        )
), combined_sources AS (
    SELECT
        api_based_companies.store_app,
        api_based_companies.app_category,
        api_based_companies.company_id,
        api_based_companies.parent_id,
        api_based_companies.ad_domain,
        api_based_companies.tag_source
    FROM api_based_companies
    UNION ALL
    SELECT
        sdk_based_companies.store_app,
        sdk_based_companies.app_category,
        sdk_based_companies.company_id,
        sdk_based_companies.parent_id,
        sdk_based_companies.ad_domain,
        sdk_based_companies.tag_source
    FROM sdk_based_companies
    UNION ALL
    SELECT
        adstxt_based_companies.store_app,
        adstxt_based_companies.app_category,
        adstxt_based_companies.company_id,
        adstxt_based_companies.parent_id,
        adstxt_based_companies.ad_domain,
        adstxt_based_companies.tag_source
    FROM adstxt_based_companies
)
SELECT
    cs.ad_domain,
    cs.store_app,
    sa.category AS app_category,
    c.id AS company_id,
    COALESCE(c.parent_company_id, c.id) AS parent_id,
    CASE
        WHEN
            (sa.sdk_successful_last_crawled IS NOT null)
            THEN BOOL_OR((cs.tag_source = 'sdk'::text))
        ELSE null::boolean
    END AS sdk,
    CASE
        WHEN
            (sa.api_successful_last_crawled IS NOT null)
            THEN BOOL_OR((cs.tag_source = 'api_call'::text))
        ELSE null::boolean
    END AS api_call,
    BOOL_OR((cs.tag_source = 'app_ads_direct'::text)) AS app_ads_direct,
    BOOL_OR((cs.tag_source = 'app_ads_reseller'::text)) AS app_ads_reseller
FROM (((
    combined_sources cs
    LEFT JOIN frontend.store_apps_overview AS sa ON ((cs.store_app = sa.id))
)
LEFT JOIN
    public.ad_domains AS ad
    ON (((cs.ad_domain)::text = (ad.domain)::text))
)
LEFT JOIN adtech.companies AS c ON ((ad.id = c.domain_id))
)
GROUP BY
    cs.ad_domain,
    cs.store_app,
    sa.category,
    c.id,
    c.parent_company_id,
    sa.sdk_successful_last_crawled,
    sa.api_successful_last_crawled
WITH NO DATA;


ALTER MATERIALIZED VIEW adtech.combined_store_apps_companies OWNER TO postgres;

--
-- Name: combined_store_app_companies_idx; Type: INDEX; Schema: adtech; Owner: postgres
--

CREATE UNIQUE INDEX combined_store_app_companies_idx ON adtech.combined_store_apps_companies USING btree (
    ad_domain, store_app, app_category, company_id
);


--
-- PostgreSQL database dump complete
--

\unrestrict 0j8bdSkxsgjFpIKVv9pJVOz3dmfWqo3gyjR8C6bOgNIOLs3cTsxeBccP87IvDcp
