--
-- PostgreSQL database dump
--

\restrict Py0DLy1zsLArCZawsAtUSEH4Sd9GGavwZp5pFBeoHst4ug6oveztGmoPqRiq243

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
-- Name: company_shares_2025_common; Type: MATERIALIZED VIEW; Schema: adtech; Owner: postgres
--

CREATE MATERIALIZED VIEW adtech.company_shares_2025_common AS
 WITH common_apps AS (
         SELECT h1.store_app
           FROM adtech.store_app_sdk_strings_2025_h1 h1
        INTERSECT
         SELECT h2.store_app
           FROM adtech.store_app_sdk_strings_2025_h2 h2
        ), h1_stats AS (
         SELECT sd.company_id,
            count(DISTINCT store_app_sdk_strings_2025_h1.store_app) AS h1_app_count
           FROM (adtech.store_app_sdk_strings_2025_h1
             JOIN adtech.sdks sd ON ((store_app_sdk_strings_2025_h1.sdk_id = sd.id)))
          WHERE (store_app_sdk_strings_2025_h1.store_app IN ( SELECT common_apps.store_app
                   FROM common_apps))
          GROUP BY sd.company_id
        ), h2_stats AS (
         SELECT sd.company_id,
            count(DISTINCT store_app_sdk_strings_2025_h2.store_app) AS h2_app_count
           FROM (adtech.store_app_sdk_strings_2025_h2
             JOIN adtech.sdks sd ON ((store_app_sdk_strings_2025_h2.sdk_id = sd.id)))
          WHERE (store_app_sdk_strings_2025_h2.store_app IN ( SELECT common_apps.store_app
                   FROM common_apps))
          GROUP BY sd.company_id
        ), comb AS (
         SELECT COALESCE(h1.company_id, h2.company_id) AS sdk_company_id,
            ( SELECT count(*) AS count
                   FROM common_apps) AS total_app_count,
            h1.h1_app_count,
            h2.h2_app_count,
            (h2.h2_app_count - h1.h1_app_count) AS net_migration,
            round((((h2.h2_app_count)::numeric / (h1.h1_app_count)::numeric) - (1)::numeric), 4) AS round
           FROM (h1_stats h1
             FULL JOIN h2_stats h2 ON ((h1.company_id = h2.company_id)))
        )
 SELECT co.sdk_company_id,
    co.total_app_count,
    co.h1_app_count,
    co.h2_app_count,
    co.net_migration,
    co.round,
    d.domain_name AS company_domain
   FROM ((comb co
     LEFT JOIN adtech.companies c ON ((co.sdk_company_id = c.id)))
     LEFT JOIN public.domains d ON ((d.id = c.domain_id)))
  WITH NO DATA;


ALTER MATERIALIZED VIEW adtech.company_shares_2025_common OWNER TO postgres;

--
-- PostgreSQL database dump complete
--

\unrestrict Py0DLy1zsLArCZawsAtUSEH4Sd9GGavwZp5pFBeoHst4ug6oveztGmoPqRiq243

