--
-- PostgreSQL database dump
--

\restrict PBFHcfeIJmSsi4r9zAphU2bfIua7GA4TeHWALBzjeJPPeA1ughJKzWsRdQqrfj9

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
-- Name: app_status; Type: MATERIALIZED VIEW; Schema: logging; Owner: postgres
--

CREATE MATERIALIZED VIEW logging.app_status AS
 WITH ranked_us_crawls AS (
         SELECT app_country_crawls.store_app,
            app_country_crawls.crawl_result,
            app_country_crawls.crawled_at,
            row_number() OVER (PARTITION BY app_country_crawls.store_app ORDER BY app_country_crawls.crawled_at DESC) AS crawl_rank
           FROM logging.app_country_crawls
          WHERE (app_country_crawls.country_id = 840)
        ), recent_us_crawls AS (
         SELECT ranked_us_crawls.store_app,
            ranked_us_crawls.crawl_result,
            ranked_us_crawls.crawled_at,
            ranked_us_crawls.crawl_rank
           FROM ranked_us_crawls
          WHERE (ranked_us_crawls.crawl_rank <= 2)
        )
 SELECT store_app,
        CASE
            WHEN ((count(*) = 2) AND (count(*) FILTER (WHERE (crawl_result = 1)) = 0)) THEN true
            ELSE false
        END AS is_removed,
    max(crawled_at) AS last_crawled_at,
    count(*) FILTER (WHERE (crawl_result = 1)) AS us_success_count_last_2_passes,
    count(*) AS total_us_passes_evaluated
   FROM recent_us_crawls
  GROUP BY store_app
  WITH NO DATA;


ALTER MATERIALIZED VIEW logging.app_status OWNER TO postgres;

--
-- PostgreSQL database dump complete
--

\unrestrict PBFHcfeIJmSsi4r9zAphU2bfIua7GA4TeHWALBzjeJPPeA1ughJKzWsRdQqrfj9

