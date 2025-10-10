--
-- PostgreSQL database dump
--

\restrict nQEOfWviezFcmoH2TIjyLqXaFq8y3kOG2AG5WlPvI4If9YkoXTATpMdINAKQdIJ

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
-- Name: store_apps_overview; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.store_apps_overview AS
WITH latest_version_codes AS (
    SELECT DISTINCT ON (version_codes.store_app)
        version_codes.id,
        version_codes.store_app,
        version_codes.version_code,
        version_codes.updated_at AS last_downloaded_at,
        version_codes.crawl_result AS download_result
    FROM public.version_codes
    WHERE
        (
            (version_codes.crawl_result = 1)
            AND (
                version_codes.updated_at
                >= '2025-05-01 00:00:00'::timestamp without time zone
            )
        )
    ORDER BY
        version_codes.store_app ASC,
        version_codes.updated_at DESC,
        (
            string_to_array((version_codes.version_code)::text, '.'::text)
        )::bigint [] DESC
), latest_successful_version_codes AS (
    SELECT DISTINCT ON (vc.store_app)
        vc.id,
        vc.store_app,
        vc.version_code,
        vc.updated_at,
        vc.crawl_result
    FROM public.version_codes AS vc
    WHERE (vc.crawl_result = 1)
    ORDER BY
        vc.store_app,
        (string_to_array((vc.version_code)::text, '.'::text))::bigint [] DESC
), last_sdk_scan AS (
    SELECT DISTINCT ON (vc.store_app)
        vc.store_app,
        lsscr.version_code_id AS version_code,
        lsscr.scanned_at,
        lsscr.scan_result
    FROM (
        public.version_code_sdk_scan_results AS lsscr
        LEFT JOIN
            public.version_codes AS vc
            ON ((lsscr.version_code_id = vc.id))
    )
    ORDER BY vc.store_app ASC, lsscr.scanned_at DESC
), last_successful_sdk_scan AS (
    SELECT DISTINCT ON (vc.store_app)
        vc.id,
        vc.store_app,
        vc.version_code,
        vcss.scanned_at,
        vcss.scan_result
    FROM (
        public.version_codes AS vc
        LEFT JOIN
            public.version_code_sdk_scan_results AS vcss
            ON ((vc.id = vcss.version_code_id))
    )
    WHERE (vcss.scan_result = 1)
    ORDER BY
        vc.store_app ASC,
        vcss.scanned_at DESC,
        (string_to_array((vc.version_code)::text, '.'::text))::bigint [] DESC
), latest_en_descriptions AS (
    SELECT DISTINCT ON (store_apps_descriptions.store_app)
        store_apps_descriptions.store_app,
        store_apps_descriptions.description,
        store_apps_descriptions.description_short
    FROM public.store_apps_descriptions
    WHERE (store_apps_descriptions.language_id = 1)
    ORDER BY
        store_apps_descriptions.store_app ASC,
        store_apps_descriptions.updated_at DESC
), latest_api_calls AS (
    SELECT DISTINCT ON (vc.store_app)
        vc.store_app,
        vasr.run_result,
        vasr.run_at
    FROM (
        public.version_codes AS vc
        LEFT JOIN
            public.version_code_api_scan_results AS vasr
            ON ((vc.id = vasr.version_code_id))
    )
    WHERE (vc.updated_at >= '2025-05-01 00:00:00'::timestamp without time zone)
), latest_successful_api_calls AS (
    SELECT DISTINCT ON (vc.store_app)
        vc.store_app,
        vasr.run_at
    FROM (
        public.version_codes AS vc
        LEFT JOIN
            public.version_code_api_scan_results AS vasr
            ON ((vc.id = vasr.version_code_id))
    )
    WHERE
        (
            (vasr.run_result = 1)
            AND (
                vc.updated_at
                >= '2025-05-01 00:00:00'::timestamp without time zone
            )
        )
), my_ad_creatives AS (
    SELECT
        cr.advertiser_store_app_id AS store_app,
        count(*) AS ad_creative_count
    FROM public.creative_records AS cr
    GROUP BY cr.advertiser_store_app_id
), my_mon_creatives AS (
    SELECT DISTINCT
        1 AS ad_mon_creatives,
        ac.store_app
    FROM (
        public.creative_records AS cr
        LEFT JOIN public.api_calls AS ac ON ((cr.api_call_id = ac.id))
    )
)
SELECT
    sa.id,
    sa.name,
    sa.store_id,
    sa.store,
    cm.mapped_category AS category,
    sa.rating,
    sa.rating_count,
    sa.review_count,
    sa.installs,
    saz.installs_sum_1w,
    saz.ratings_sum_1w,
    saz.installs_sum_4w,
    saz.ratings_sum_4w,
    saz.installs_z_score_2w,
    saz.ratings_z_score_2w,
    saz.installs_z_score_4w,
    saz.ratings_z_score_4w,
    sa.ad_supported,
    sa.in_app_purchases,
    sa.store_last_updated,
    sa.created_at,
    sa.updated_at,
    sa.crawl_result,
    sa.icon_url_100,
    sa.icon_url_512,
    sa.release_date,
    sa.featured_image_url,
    sa.phone_image_url_1,
    sa.phone_image_url_2,
    sa.phone_image_url_3,
    sa.tablet_image_url_1,
    sa.tablet_image_url_2,
    sa.tablet_image_url_3,
    d.developer_id,
    d.name AS developer_name,
    pd.domain_name AS developer_url,
    pdcr.updated_at AS adstxt_last_crawled,
    pdcr.crawl_result AS adstxt_crawl_result,
    lss.scanned_at AS sdk_last_crawled,
    lss.scan_result AS sdk_last_crawl_result,
    lsss.scanned_at AS sdk_successful_last_crawled,
    lvc.version_code,
    ld.description,
    ld.description_short,
    lac.run_at AS api_last_crawled,
    lac.run_result,
    lsac.run_at AS api_successful_last_crawled,
    acr.ad_creative_count,
    amc.ad_mon_creatives
FROM (((((((((((((((
    public.store_apps sa
    LEFT JOIN
        public.category_mapping AS cm
        ON (((sa.category)::text = (cm.original_category)::text))
)
LEFT JOIN public.developers AS d ON ((sa.developer = d.id))
)
LEFT JOIN public.app_urls_map AS aum ON ((sa.id = aum.store_app))
)
LEFT JOIN public.domains AS pd ON ((aum.pub_domain = pd.id))
)
LEFT JOIN public.adstxt_crawl_results AS pdcr ON ((pd.id = pdcr.domain_id))
)
LEFT JOIN latest_version_codes AS lvc ON ((sa.id = lvc.store_app))
)
LEFT JOIN last_sdk_scan AS lss ON ((sa.id = lss.store_app))
)
LEFT JOIN last_successful_sdk_scan AS lsss ON ((sa.id = lsss.store_app))
)
LEFT JOIN latest_successful_version_codes AS lsvc ON ((sa.id = lsvc.store_app))
)
LEFT JOIN latest_en_descriptions AS ld ON ((sa.id = ld.store_app))
)
LEFT JOIN public.store_app_z_scores AS saz ON ((sa.id = saz.store_app))
)
LEFT JOIN latest_api_calls AS lac ON ((sa.id = lac.store_app))
)
LEFT JOIN latest_successful_api_calls AS lsac ON ((sa.id = lsac.store_app))
)
LEFT JOIN my_ad_creatives AS acr ON ((sa.id = acr.store_app))
)
LEFT JOIN my_mon_creatives AS amc ON ((sa.id = amc.store_app))
)
WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.store_apps_overview OWNER TO postgres;

--
-- Name: store_apps_overview_unique_idx; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE UNIQUE INDEX store_apps_overview_unique_idx ON frontend.store_apps_overview USING btree (
    store, store_id
);


--
-- PostgreSQL database dump complete
--

\unrestrict nQEOfWviezFcmoH2TIjyLqXaFq8y3kOG2AG5WlPvI4If9YkoXTATpMdINAKQdIJ
