--
-- PostgreSQL database dump
--

\restrict b1rApWavPgJdehH8hIoloJHSzZiCxTKMqT9lIeqEZtRm0xE2G8epGEtkj9QpyKq

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
-- Name: companies_apps_overview; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.companies_apps_overview AS
 WITH store_app_sdk_companies AS (
         SELECT DISTINCT savs.store_app,
            savs.company_id
           FROM adtech.store_app_sdk_strings savs
        )
 SELECT sa.store_id,
    sacs.company_id,
    c.name AS company_name,
    d.domain_name AS company_domain,
    cc2.url_slug AS category_slug
   FROM (((((store_app_sdk_companies sacs
     LEFT JOIN public.store_apps sa ON ((sacs.store_app = sa.id)))
     LEFT JOIN adtech.companies c ON ((sacs.company_id = c.id)))
     LEFT JOIN public.domains d ON ((c.domain_id = d.id)))
     LEFT JOIN adtech.company_categories cc ON ((c.id = cc.company_id)))
     LEFT JOIN adtech.categories cc2 ON ((cc.category_id = cc2.id)))
  WHERE (sacs.company_id IS NOT NULL)
  WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.companies_apps_overview OWNER TO postgres;

--
-- Name: companies_apps_overview_idx; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE INDEX companies_apps_overview_idx ON frontend.companies_apps_overview USING btree (store_id);


--
-- Name: companies_apps_overview_unique_idx; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE UNIQUE INDEX companies_apps_overview_unique_idx ON frontend.companies_apps_overview USING btree (store_id, company_id, category_slug);


--
-- PostgreSQL database dump complete
--

\unrestrict b1rApWavPgJdehH8hIoloJHSzZiCxTKMqT9lIeqEZtRm0xE2G8epGEtkj9QpyKq

