--
-- PostgreSQL database dump
--

\restrict HQcia6x1xIyaECYMwi9UBEgs4e4UkE0id2Fr4n5hkNcqqbKrp3E73c6jubdRnfb

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
-- Name: companies_sdks_overview; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.companies_sdks_overview AS
SELECT
    c.name AS company_name,
    ad.domain_name AS company_domain,
    parad.domain_name AS parent_company_domain,
    sdk.sdk_name,
    sp.package_pattern,
    sp2.path_pattern,
    COALESCE(cc.name, c.name) AS parent_company_name
FROM ((((((
    adtech.companies c
    LEFT JOIN adtech.companies AS cc ON ((c.parent_company_id = cc.id))
)
LEFT JOIN public.domains AS ad ON ((c.domain_id = ad.id))
)
LEFT JOIN public.domains AS parad ON ((cc.domain_id = parad.id))
)
LEFT JOIN adtech.sdks AS sdk ON ((c.id = sdk.company_id))
)
LEFT JOIN adtech.sdk_packages AS sp ON ((sdk.id = sp.sdk_id))
)
LEFT JOIN adtech.sdk_paths AS sp2 ON ((sdk.id = sp2.sdk_id))
)
WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.companies_sdks_overview OWNER TO postgres;

--
-- Name: companies_sdks_overview_unique_idx; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE UNIQUE INDEX companies_sdks_overview_unique_idx ON frontend.companies_sdks_overview USING btree (
    company_name,
    company_domain,
    parent_company_domain,
    sdk_name,
    package_pattern,
    path_pattern
);


--
-- PostgreSQL database dump complete
--

\unrestrict HQcia6x1xIyaECYMwi9UBEgs4e4UkE0id2Fr4n5hkNcqqbKrp3E73c6jubdRnfb
