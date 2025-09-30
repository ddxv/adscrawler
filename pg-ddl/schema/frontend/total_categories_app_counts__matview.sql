--
-- PostgreSQL database dump
--

\restrict BmJdb2goqBRTRDtgfQfbucpHXPoIns5EXcwNXyIbmVODk7f6tpZaJ9adGNXxbqm

-- Dumped from database version 17.6 (Ubuntu 17.6-2.pgdg24.04+1)
-- Dumped by pg_dump version 17.6 (Ubuntu 17.6-2.pgdg24.04+1)

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
-- Name: total_categories_app_counts; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.total_categories_app_counts AS
SELECT
    sa.store,
    tag.tag_source,
    csac.app_category,
    count(DISTINCT csac.store_app) AS app_count
FROM ((
    adtech.combined_store_apps_companies csac
    LEFT JOIN public.store_apps AS sa ON ((csac.store_app = sa.id))
)
CROSS JOIN
    LATERAL (
        VALUES ('sdk'::text, csac.sdk),
        ('api_call'::text, csac.api_call),
        ('app_ads_direct'::text, csac.app_ads_direct),
        ('app_ads_reseller'::text, csac.app_ads_reseller)
    ) AS tag (tag_source, present)
)
WHERE (tag.present IS true)
GROUP BY sa.store, tag.tag_source, csac.app_category
WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.total_categories_app_counts OWNER TO postgres;

--
-- Name: idx_total_categories_app_counts; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE UNIQUE INDEX idx_total_categories_app_counts ON frontend.total_categories_app_counts USING btree (
    store, tag_source, app_category
);


--
-- PostgreSQL database dump complete
--

\unrestrict BmJdb2goqBRTRDtgfQfbucpHXPoIns5EXcwNXyIbmVODk7f6tpZaJ9adGNXxbqm
