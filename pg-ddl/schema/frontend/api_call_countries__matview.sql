--
-- PostgreSQL database dump
--

\restrict wIhbU7ZJgdzEEuOU0eC4xef8cd4ixqEM5CC3VgGgMuT7d34naTR4z4n36Hq0ubo

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
-- Name: api_call_countries; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.api_call_countries AS
WITH latest_run_per_app AS (
    SELECT DISTINCT ON (saac.store_app)
        saac.store_app,
        saac.run_id
    FROM (
        public.store_app_api_calls AS saac
        INNER JOIN
            public.version_code_api_scan_results AS vcasr
            ON ((saac.run_id = vcasr.id))
    )
    WHERE (saac.country_id IS NOT null)
    ORDER BY saac.store_app ASC, vcasr.run_at DESC
), filtered_calls AS (
    SELECT
        saac.store_app,
        saac.tld_url,
        saac.url,
        saac.country_id,
        saac.city_name,
        saac.org
    FROM (
        public.store_app_api_calls AS saac
        INNER JOIN
            latest_run_per_app AS lra
            ON
                (
                    (
                        (saac.store_app = lra.store_app)
                        AND (saac.run_id = lra.run_id)
                    )
                )
    )
), cleaned_calls AS (
    SELECT
        filtered_calls.store_app,
        filtered_calls.tld_url,
        filtered_calls.country_id,
        filtered_calls.city_name,
        filtered_calls.org,
        regexp_replace(
            regexp_replace(
                regexp_replace(
                    filtered_calls.url, '^https?://'::text, ''::text
                ),
                '\?.*$'::text,
                ''::text
            ),
            '^(([^/]+/){0,2}[^/]+).*$'::text,
            '\1'::text
        ) AS short_url
    FROM filtered_calls
)
SELECT
    ca.tld_url,
    co.alpha2 AS country,
    ca.org,
    coalesce(cad.domain, (ca.tld_url)::character varying) AS company_domain,
    coalesce(
        pcad.domain, coalesce(cad.domain, (ca.tld_url)::character varying)
    ) AS parent_company_domain,
    count(DISTINCT ca.store_app) AS store_app_count
FROM (((((((
    cleaned_calls ca
    LEFT JOIN public.ad_domains AS ad ON ((ca.tld_url = (ad.domain)::text))
)
LEFT JOIN adtech.company_domain_mapping AS cdm ON ((ad.id = cdm.domain_id))
)
LEFT JOIN adtech.companies AS c ON ((cdm.company_id = c.id))
)
LEFT JOIN public.ad_domains AS cad ON ((c.domain_id = cad.id))
)
LEFT JOIN adtech.companies AS pc ON ((c.parent_company_id = pc.id))
)
LEFT JOIN public.ad_domains AS pcad ON ((pc.domain_id = pcad.id))
)
LEFT JOIN public.countries AS co ON ((ca.country_id = co.id))
)
GROUP BY
    coalesce(cad.domain, (ca.tld_url)::character varying),
    coalesce(
        pcad.domain, coalesce(cad.domain, (ca.tld_url)::character varying)
    ),
    ca.tld_url,
    co.alpha2,
    ca.org
ORDER BY (count(DISTINCT ca.store_app)) DESC
WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.api_call_countries OWNER TO postgres;

--
-- Name: api_call_countries_unique; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE UNIQUE INDEX api_call_countries_unique ON frontend.api_call_countries USING btree (
    company_domain, parent_company_domain, tld_url, country, org
);


--
-- PostgreSQL database dump complete
--

\unrestrict wIhbU7ZJgdzEEuOU0eC4xef8cd4ixqEM5CC3VgGgMuT7d34naTR4z4n36Hq0ubo
