--
-- PostgreSQL database dump
--

\restrict n2G2TZTieTPI9XJVc6cvNOPFct6hwh8eeo7apOONYzu8XO5V51L34Rl2k6shFGZ

-- Dumped from database version 18.6 (Ubuntu 18.6-1.pgdg26.04+2)
-- Dumped by pg_dump version 18.6 (Ubuntu 18.6-1.pgdg26.04+2)

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
 WITH combined_patterns AS (
         SELECT sp.sdk_id,
            'package'::text AS pattern_type,
            sp.package_pattern AS pattern_value,
            max(sss.app_count) AS pattern_app_count
           FROM ((adtech.sdk_packages sp
             JOIN public.version_strings vs ON ((vs.value_name ~~ ((sp.package_pattern)::text || '%'::text))))
             JOIN adtech.sdk_string_stats sss ON (((sss.string_id = vs.id) AND (sss.sdk_id = sp.sdk_id))))
          GROUP BY sp.sdk_id, sp.package_pattern
        UNION ALL
         SELECT sp.sdk_id,
            'path'::text AS pattern_type,
            sp.path_pattern AS pattern_value,
            max(sss.app_count) AS pattern_app_count
           FROM ((adtech.sdk_paths sp
             JOIN public.version_strings vs ON ((vs.xml_path ~~ (('%'::text || (sp.path_pattern)::text) || '%'::text))))
             JOIN adtech.sdk_string_stats sss ON (((sss.string_id = vs.id) AND (sss.sdk_id = sp.sdk_id))))
          GROUP BY sp.sdk_id, sp.path_pattern
        UNION ALL
         SELECT smp.sdk_id,
            'mediation'::text AS pattern_type,
            smp.mediation_pattern AS pattern_value,
            max(sss.app_count) AS pattern_app_count
           FROM ((adtech.sdk_mediation_patterns smp
             JOIN public.version_strings vs ON ((vs.value_name ~~ ((smp.mediation_pattern)::text || '%'::text))))
             JOIN adtech.sdk_string_stats sss ON (((sss.string_id = vs.id) AND (sss.sdk_id = smp.sdk_id))))
          GROUP BY smp.sdk_id, smp.mediation_pattern
        )
 SELECT c.name AS company_name,
    ad.domain_name AS company_domain,
    parad.domain_name AS parent_company_domain,
    sdk.id AS sdk_id,
    sdk.sdk_name,
    sdk.sdk_slug,
    cp.pattern_type,
    cp.pattern_value,
    cp.pattern_app_count AS app_count,
    COALESCE(cc.name, c.name) AS parent_company_name
   FROM (((((adtech.sdks sdk
     JOIN adtech.companies c ON ((c.id = sdk.company_id)))
     LEFT JOIN adtech.companies cc ON ((cc.id = c.parent_company_id)))
     LEFT JOIN public.domains ad ON ((ad.id = c.domain_id)))
     LEFT JOIN public.domains parad ON ((parad.id = cc.domain_id)))
     JOIN combined_patterns cp ON ((cp.sdk_id = sdk.id)))
  WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.companies_sdks_overview OWNER TO postgres;

--
-- Name: companies_sdks_overview_unique_idx; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE UNIQUE INDEX companies_sdks_overview_unique_idx ON frontend.companies_sdks_overview USING btree (company_name, company_domain, parent_company_domain, sdk_name, pattern_type, pattern_value);


--
-- Name: idx_cso_company_domain; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE INDEX idx_cso_company_domain ON frontend.companies_sdks_overview USING btree (company_domain);


--
-- Name: idx_cso_parent_company_domain; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE INDEX idx_cso_parent_company_domain ON frontend.companies_sdks_overview USING btree (parent_company_domain);


--
-- PostgreSQL database dump complete
--

\unrestrict n2G2TZTieTPI9XJVc6cvNOPFct6hwh8eeo7apOONYzu8XO5V51L34Rl2k6shFGZ

