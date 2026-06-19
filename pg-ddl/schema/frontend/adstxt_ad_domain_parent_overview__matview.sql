--
-- PostgreSQL database dump
--

\restrict gEQqbBO7CCrjxU43ThSKaIXeyBQTzq47uoKN7aKwIEcPT4e0abIbapUtU2mfxog

-- Dumped from database version 18.3 (Ubuntu 18.3-1.pgdg24.04+1)
-- Dumped by pg_dump version 18.3 (Ubuntu 18.3-1.pgdg24.04+1)

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
-- Name: adstxt_ad_domain_parent_overview; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.adstxt_ad_domain_parent_overview AS
 WITH child_companies AS (
         SELECT pc_1.id AS parent_company_id,
            aae.relationship,
            sa.store,
            aae.publisher_id,
            sa.developer,
            aesa.store_app
           FROM (((((frontend.adstxt_entries_store_apps aesa
             LEFT JOIN public.store_apps sa ON ((aesa.store_app = sa.id)))
             LEFT JOIN public.app_ads_entrys aae ON ((aesa.app_ad_entry_id = aae.id)))
             LEFT JOIN adtech.company_domain_mapping cdm ON ((aesa.ad_domain_id = cdm.domain_id)))
             LEFT JOIN adtech.companies c ON ((cdm.company_id = c.id)))
             JOIN adtech.companies pc_1 ON ((c.parent_company_id = pc_1.id)))
          WHERE (c.parent_company_id IS NOT NULL)
        ), parent_companies_direct AS (
         SELECT c.id AS parent_company_id,
            aae.relationship,
            sa.store,
            aae.publisher_id,
            sa.developer,
            aesa.store_app
           FROM ((((frontend.adstxt_entries_store_apps aesa
             LEFT JOIN public.store_apps sa ON ((aesa.store_app = sa.id)))
             LEFT JOIN public.app_ads_entrys aae ON ((aesa.app_ad_entry_id = aae.id)))
             LEFT JOIN adtech.company_domain_mapping cdm ON ((aesa.ad_domain_id = cdm.domain_id)))
             JOIN adtech.companies c ON ((cdm.company_id = c.id)))
          WHERE ((c.parent_company_id IS NULL) AND (EXISTS ( SELECT 1
                   FROM adtech.companies child
                  WHERE (child.parent_company_id = c.id))))
        )
 SELECT ad.domain_name AS ad_domain_url,
    combined.relationship,
    combined.store,
    count(DISTINCT combined.publisher_id) AS publisher_id_count,
    count(DISTINCT combined.developer) AS developer_count,
    count(DISTINCT combined.store_app) AS app_count
   FROM ((( SELECT child_companies.parent_company_id,
            child_companies.relationship,
            child_companies.store,
            child_companies.publisher_id,
            child_companies.developer,
            child_companies.store_app
           FROM child_companies
        UNION ALL
         SELECT parent_companies_direct.parent_company_id,
            parent_companies_direct.relationship,
            parent_companies_direct.store,
            parent_companies_direct.publisher_id,
            parent_companies_direct.developer,
            parent_companies_direct.store_app
           FROM parent_companies_direct) combined
     LEFT JOIN adtech.companies pc ON ((combined.parent_company_id = pc.id)))
     LEFT JOIN public.domains ad ON ((pc.domain_id = ad.id)))
  GROUP BY ad.domain_name, combined.relationship, combined.store
  WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.adstxt_ad_domain_parent_overview OWNER TO postgres;

--
-- Name: adstxt_ad_domain_parent_overview_idx; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE INDEX adstxt_ad_domain_parent_overview_idx ON frontend.adstxt_ad_domain_parent_overview USING btree (ad_domain_url);


--
-- Name: adstxt_ad_domain_parent_overview_unique_idx; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE UNIQUE INDEX adstxt_ad_domain_parent_overview_unique_idx ON frontend.adstxt_ad_domain_parent_overview USING btree (ad_domain_url, relationship, store);


--
-- PostgreSQL database dump complete
--

\unrestrict gEQqbBO7CCrjxU43ThSKaIXeyBQTzq47uoKN7aKwIEcPT4e0abIbapUtU2mfxog

