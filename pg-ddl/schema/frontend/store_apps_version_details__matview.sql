--
-- PostgreSQL database dump
--

\restrict rwCGYkJuzkEq2dwehE1ksGhFPS1Mv8nJFIADPe54aEDv0FiRknN6NffOsNCgv4Q

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
-- Name: store_apps_version_details; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.store_apps_version_details AS
WITH latest_version_codes AS (
    SELECT DISTINCT ON (version_codes.store_app)
        version_codes.id,
        version_codes.store_app,
        version_codes.version_code,
        version_codes.updated_at,
        version_codes.crawl_result
    FROM public.version_codes
    ORDER BY
        version_codes.store_app,
        (
            string_to_array((version_codes.version_code)::text, '.'::text)
        )::bigint [] DESC
)
SELECT DISTINCT
    vs.id AS version_string_id,
    sa.store,
    sa.store_id,
    cvsm.company_id,
    c.name AS company_name,
    ad.domain AS company_domain,
    cats.url_slug AS category_slug
FROM ((((((((
    latest_version_codes vc
    LEFT JOIN public.version_details_map AS vdm ON ((vc.id = vdm.version_code))
)
LEFT JOIN public.version_strings AS vs ON ((vdm.string_id = vs.id))
)
LEFT JOIN
    adtech.company_value_string_mapping AS cvsm
    ON ((vs.id = cvsm.version_string_id))
)
LEFT JOIN adtech.companies AS c ON ((cvsm.company_id = c.id))
)
LEFT JOIN adtech.company_categories AS cc ON ((c.id = cc.company_id))
)
LEFT JOIN adtech.categories AS cats ON ((cc.category_id = cats.id))
)
LEFT JOIN public.ad_domains AS ad ON ((c.domain_id = ad.id))
)
LEFT JOIN public.store_apps AS sa ON ((vc.store_app = sa.id))
)
WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.store_apps_version_details OWNER TO postgres;

--
-- Name: store_apps_version_details_store_id_idx; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE INDEX store_apps_version_details_store_id_idx ON frontend.store_apps_version_details USING btree (
    store_id
);


--
-- Name: store_apps_version_details_unique_idx; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE UNIQUE INDEX store_apps_version_details_unique_idx ON frontend.store_apps_version_details USING btree (
    version_string_id, store_id, company_id, company_domain, category_slug
);


--
-- PostgreSQL database dump complete
--

\unrestrict rwCGYkJuzkEq2dwehE1ksGhFPS1Mv8nJFIADPe54aEDv0FiRknN6NffOsNCgv4Q
