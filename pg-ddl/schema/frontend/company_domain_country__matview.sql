--
-- PostgreSQL database dump
--

\restrict Nm2MC1igJTHB60gg6FgNXmsaT8hdwEb0oXZDmpHLaznNyYJOr7KpdsFNAMbwHbF

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
-- Name: company_domain_country; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.company_domain_country AS
WITH country_totals AS (
    SELECT
        api_call_countries.company_domain,
        api_call_countries.country,
        sum(api_call_countries.store_app_count) AS total_app_count
    FROM frontend.api_call_countries
    GROUP BY api_call_countries.company_domain, api_call_countries.country
), parent_country_totals AS (
    SELECT
        api_call_countries.parent_company_domain,
        api_call_countries.country,
        sum(api_call_countries.store_app_count) AS total_app_count
    FROM frontend.api_call_countries
    GROUP BY
        api_call_countries.parent_company_domain, api_call_countries.country
), parent_ranked_countries AS (
    SELECT
        parent_country_totals.parent_company_domain,
        parent_country_totals.country,
        parent_country_totals.total_app_count,
        row_number()
            OVER (
                PARTITION BY parent_country_totals.parent_company_domain
                ORDER BY parent_country_totals.total_app_count DESC
            )
            AS rn
    FROM parent_country_totals
), company_ranked_countries AS (
    SELECT
        country_totals.company_domain,
        country_totals.country,
        country_totals.total_app_count,
        row_number()
            OVER (
                PARTITION BY country_totals.company_domain
                ORDER BY country_totals.total_app_count DESC
            )
            AS rn
    FROM country_totals
)
SELECT
    company_ranked_countries.company_domain,
    company_ranked_countries.country AS most_common_country,
    company_ranked_countries.total_app_count
FROM company_ranked_countries
WHERE ((NOT ((
    company_ranked_countries.company_domain)::text IN (
    SELECT parent_ranked_countries.parent_company_domain
    FROM parent_ranked_countries
))) AND (company_ranked_countries.rn = 1))
UNION
SELECT
    parent_ranked_countries.parent_company_domain AS company_domain,
    parent_ranked_countries.country AS most_common_country,
    parent_ranked_countries.total_app_count
FROM parent_ranked_countries
WHERE (parent_ranked_countries.rn = 1)
WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.company_domain_country OWNER TO postgres;

--
-- PostgreSQL database dump complete
--

\unrestrict Nm2MC1igJTHB60gg6FgNXmsaT8hdwEb0oXZDmpHLaznNyYJOr7KpdsFNAMbwHbF
