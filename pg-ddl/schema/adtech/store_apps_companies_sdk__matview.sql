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
-- Name: store_apps_companies_sdk; Type: MATERIALIZED VIEW; Schema: adtech; Owner: postgres
--

CREATE MATERIALIZED VIEW adtech.store_apps_companies_sdk AS
 WITH latest_version_codes AS (
         SELECT DISTINCT ON (version_codes.store_app) version_codes.id,
            version_codes.store_app,
            version_codes.version_code
           FROM public.version_codes
          WHERE (version_codes.crawl_result = 1)
          ORDER BY version_codes.store_app, (string_to_array((version_codes.version_code)::text, '.'::text))::bigint[] DESC
        ), sdk_apps_with_companies AS (
         SELECT DISTINCT vc.store_app,
            cvsm.company_id,
            COALESCE(pc.parent_company_id, cvsm.company_id) AS parent_id
           FROM (((latest_version_codes vc
             LEFT JOIN public.version_details_map vdm ON ((vc.id = vdm.version_code)))
             JOIN adtech.company_value_string_mapping cvsm ON ((vdm.string_id = cvsm.version_string_id)))
             LEFT JOIN adtech.companies pc ON ((cvsm.company_id = pc.id)))
        ), sdk_paths_with_companies AS (
         SELECT DISTINCT vc.store_app,
            sd.company_id,
            COALESCE(pc.parent_company_id, sd.company_id) AS parent_id
           FROM (((((latest_version_codes vc
             LEFT JOIN public.version_details_map vdm ON ((vc.id = vdm.version_code)))
             LEFT JOIN public.version_strings vs ON ((vdm.string_id = vs.id)))
             JOIN adtech.sdk_paths ptm ON ((vs.value_name ~~* ((ptm.path_pattern)::text || '%'::text))))
             LEFT JOIN adtech.sdks sd ON ((ptm.sdk_id = sd.id)))
             LEFT JOIN adtech.companies pc ON ((sd.company_id = pc.id)))
        ), dev_apps_with_companies AS (
         SELECT DISTINCT sa.id AS store_app,
            cd.company_id,
            COALESCE(pc.parent_company_id, cd.company_id) AS parent_id
           FROM ((adtech.company_developers cd
             LEFT JOIN public.store_apps sa ON ((cd.developer_id = sa.developer)))
             LEFT JOIN adtech.companies pc ON ((cd.company_id = pc.id)))
        ), all_apps_with_companies AS (
         SELECT sawc.store_app,
            sawc.company_id,
            sawc.parent_id
           FROM sdk_apps_with_companies sawc
        UNION
         SELECT spwc.store_app,
            spwc.company_id,
            spwc.parent_id
           FROM sdk_paths_with_companies spwc
        UNION
         SELECT dawc.store_app,
            dawc.company_id,
            dawc.parent_id
           FROM dev_apps_with_companies dawc
        ), distinct_apps_with_cats AS (
         SELECT DISTINCT aawc.store_app,
            c.id AS category_id
           FROM ((all_apps_with_companies aawc
             LEFT JOIN adtech.company_categories cc ON ((aawc.company_id = cc.company_id)))
             LEFT JOIN adtech.categories c ON ((cc.category_id = c.id)))
        ), distinct_store_apps AS (
         SELECT DISTINCT lvc.store_app
           FROM latest_version_codes lvc
        ), all_combinations AS (
         SELECT sa.store_app,
            c.id AS category_id
           FROM (distinct_store_apps sa
             CROSS JOIN adtech.categories c)
        ), unmatched_apps AS (
         SELECT DISTINCT ac.store_app,
            (- ac.category_id) AS company_id,
            (- ac.category_id) AS parent_id
           FROM (all_combinations ac
             LEFT JOIN distinct_apps_with_cats dawc ON (((ac.store_app = dawc.store_app) AND (ac.category_id = dawc.category_id))))
          WHERE (dawc.store_app IS NULL)
        ), final_union AS (
         SELECT aawc.store_app,
            aawc.company_id,
            aawc.parent_id
           FROM all_apps_with_companies aawc
        UNION
         SELECT ua.store_app,
            ua.company_id,
            ua.parent_id
           FROM unmatched_apps ua
        )
 SELECT store_app,
    company_id,
    parent_id
   FROM final_union
  WITH NO DATA;


ALTER MATERIALIZED VIEW adtech.store_apps_companies_sdk OWNER TO postgres;

--
-- Name: idx_store_apps_companies_sdk_new; Type: INDEX; Schema: adtech; Owner: postgres
--

CREATE UNIQUE INDEX idx_store_apps_companies_sdk_new ON adtech.store_apps_companies_sdk USING btree (store_app, company_id, parent_id);


--
-- PostgreSQL database dump complete
--

