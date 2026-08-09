--
-- PostgreSQL database dump
--

\restrict wOJjnfXfTQAl7TGcHVpsH4H3XPCiP28ehTSYzzS2ac0e5Yq889KopP2Yobeu6Gy

-- Dumped from database version 18.4 (Ubuntu 18.4-1.pgdg26.04+1)
-- Dumped by pg_dump version 18.4 (Ubuntu 18.4-1.pgdg26.04+1)

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
-- Name: category_type_country_stats; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.category_type_country_stats AS
 WITH mygroups AS (
         SELECT DISTINCT cctts.store,
            cctts.app_category,
            cctts.company_domain,
            cctts.type_url_slug,
            co.country
           FROM (frontend.companies_category_tag_type_stats cctts
             LEFT JOIN frontend.companies_overview co ON (((co.company_domain)::text = (cctts.company_domain)::text)))
          WHERE (cctts.app_count > 0)
        )
 SELECT store,
    app_category,
    type_url_slug,
    country,
    count(*) AS company_count
   FROM mygroups
  GROUP BY store, app_category, type_url_slug, country
  WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.category_type_country_stats OWNER TO postgres;

--
-- PostgreSQL database dump complete
--

\unrestrict wOJjnfXfTQAl7TGcHVpsH4H3XPCiP28ehTSYzzS2ac0e5Yq889KopP2Yobeu6Gy

