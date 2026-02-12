--
-- PostgreSQL database dump
--

\restrict luGg0AZP3qibbOErwv49F9CfY6KvCqONUVIZ3aEzIz04LS9Gn8bJ3FEPDn5wcbr

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
-- Name: mediation_adapter_app_counts; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.mediation_adapter_app_counts AS
 WITH filter_mediation_strings AS (
         SELECT vs.id AS string_id,
            sd.company_id AS mediation_company_id,
            vs.value_name AS full_sdk,
            regexp_replace(regexp_replace(vs.value_name, concat(cmp.mediation_pattern, '.'), ''::text), '\..*$'::text, ''::text) AS adapter_string
           FROM ((public.version_strings vs
             JOIN adtech.sdk_mediation_patterns cmp ON ((lower(vs.value_name) ~~ (lower(concat((cmp.mediation_pattern)::text, '.')) || '%'::text))))
             JOIN adtech.sdks sd ON ((cmp.sdk_id = sd.id)))
        ), mediation_strings AS (
         SELECT fms.string_id,
            fms.mediation_company_id,
            cma.company_id AS adapter_company_id,
            fms.adapter_string,
            fms.full_sdk
           FROM (filter_mediation_strings fms
             LEFT JOIN adtech.company_mediation_adapters cma ON ((lower(fms.adapter_string) ~~ (lower((cma.adapter_pattern)::text) || '%'::text))))
          WHERE (fms.mediation_company_id <> cma.company_id)
        ), app_counts AS (
         SELECT ms.mediation_company_id,
            ms.adapter_string,
            ms.adapter_company_id,
            cm.mapped_category AS app_category,
            count(DISTINCT sass.store_app) AS app_count
           FROM (((adtech.store_app_sdk_strings sass
             JOIN mediation_strings ms ON ((sass.version_string_id = ms.string_id)))
             LEFT JOIN public.store_apps sa ON ((sass.store_app = sa.id)))
             LEFT JOIN public.category_mapping cm ON (((sa.category)::text = (cm.original_category)::text)))
          GROUP BY ms.mediation_company_id, ms.adapter_string, ms.adapter_company_id, cm.mapped_category
        )
 SELECT md.domain_name AS mediation_domain,
    ac.adapter_string,
    ad.domain_name AS adapter_domain,
    adc.name AS adapter_company_name,
    adc.logo_url AS adapter_logo_url,
    ac.app_category,
    ac.app_count
   FROM ((((app_counts ac
     LEFT JOIN adtech.companies mdc ON ((ac.mediation_company_id = mdc.id)))
     LEFT JOIN public.domains md ON ((mdc.domain_id = md.id)))
     LEFT JOIN adtech.companies adc ON ((ac.adapter_company_id = adc.id)))
     LEFT JOIN public.domains ad ON ((adc.domain_id = ad.id)))
  WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.mediation_adapter_app_counts OWNER TO postgres;

--
-- PostgreSQL database dump complete
--

\unrestrict luGg0AZP3qibbOErwv49F9CfY6KvCqONUVIZ3aEzIz04LS9Gn8bJ3FEPDn5wcbr

