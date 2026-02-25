--
-- PostgreSQL database dump
--

\restrict vJhozM8BblIFUme88Z1bC7vDN8SV7IcCiWI4DNfzZ9vMxCUlf4mMdTsUZi9jM30

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
-- Name: companies_category_stats; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.companies_category_stats AS
 SELECT sa.store,
    sa.category AS app_category,
    csac.ad_domain AS company_domain,
    c.name AS company_name,
    count(DISTINCT csac.store_app) AS app_count,
    sum(sa.installs) AS installs_total,
    sum(sa.installs_sum_4w) AS installs_d30
   FROM ((adtech.combined_store_apps_companies csac
     LEFT JOIN adtech.companies c ON ((csac.company_id = c.id)))
     LEFT JOIN frontend.store_apps_overview sa ON ((csac.store_app = sa.id)))
  GROUP BY sa.store, sa.category, csac.ad_domain, c.name
  WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.companies_category_stats OWNER TO postgres;

--
-- Name: companies_category_stats_idx; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE UNIQUE INDEX companies_category_stats_idx ON frontend.companies_category_stats USING btree (store, app_category, company_domain);


--
-- Name: companies_category_stats_query_idx; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE INDEX companies_category_stats_query_idx ON frontend.companies_category_stats USING btree (company_domain);


--
-- PostgreSQL database dump complete
--

\unrestrict vJhozM8BblIFUme88Z1bC7vDN8SV7IcCiWI4DNfzZ9vMxCUlf4mMdTsUZi9jM30

