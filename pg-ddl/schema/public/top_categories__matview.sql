--
-- PostgreSQL database dump
--

\restrict Sdi4dqGkIHrDiLqpsY8hUYmC5hUSeij8fjUwPQNOkNahujhuWCRe2ZCPkMQJrPu

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
-- Name: top_categories; Type: MATERIALIZED VIEW; Schema: public; Owner: postgres
--

CREATE MATERIALIZED VIEW public.top_categories AS
WITH rankedapps AS (
    SELECT
        sa.id,
        sa.developer,
        sa.name,
        sa.store_id,
        sa.store,
        sa.category,
        sa.rating,
        sa.review_count,
        sa.installs,
        sa.free,
        sa.price,
        sa.size,
        sa.minimum_android,
        sa.developer_email,
        sa.store_last_updated,
        sa.content_rating,
        sa.ad_supported,
        sa.in_app_purchases,
        sa.editors_choice,
        sa.created_at,
        sa.updated_at,
        sa.crawl_result,
        sa.icon_url_512,
        sa.release_date,
        sa.rating_count,
        sa.featured_image_url,
        sa.phone_image_url_1,
        sa.phone_image_url_2,
        sa.phone_image_url_3,
        sa.tablet_image_url_1,
        sa.tablet_image_url_2,
        sa.tablet_image_url_3,
        cm.original_category,
        cm.mapped_category,
        row_number()
            OVER (
                PARTITION BY sa.store, cm.mapped_category
                ORDER BY
                    sa.installs DESC NULLS LAST, sa.rating_count DESC NULLS LAST
            )
            AS rn
    FROM (
        public.store_apps AS sa
        INNER JOIN
            public.category_mapping AS cm
            ON (((sa.category)::text = (cm.original_category)::text))
    )
    WHERE (sa.crawl_result = 1)
)
SELECT
    id,
    developer,
    name,
    store_id,
    store,
    category,
    rating,
    review_count,
    installs,
    free,
    price,
    size,
    minimum_android,
    developer_email,
    store_last_updated,
    content_rating,
    ad_supported,
    in_app_purchases,
    editors_choice,
    created_at,
    updated_at,
    crawl_result,
    icon_url_512,
    release_date,
    rating_count,
    featured_image_url,
    phone_image_url_1,
    phone_image_url_2,
    phone_image_url_3,
    tablet_image_url_1,
    tablet_image_url_2,
    tablet_image_url_3,
    original_category,
    mapped_category,
    rn
FROM rankedapps
WHERE (rn <= 50)
WITH NO DATA;


ALTER MATERIALIZED VIEW public.top_categories OWNER TO postgres;

--
-- Name: idx_top_categories; Type: INDEX; Schema: public; Owner: postgres
--

CREATE UNIQUE INDEX idx_top_categories ON public.top_categories USING btree (
    store, mapped_category, store_id
);


--
-- PostgreSQL database dump complete
--

\unrestrict Sdi4dqGkIHrDiLqpsY8hUYmC5hUSeij8fjUwPQNOkNahujhuWCRe2ZCPkMQJrPu
