--
-- PostgreSQL database dump
--

-- Dumped from database version 17.5 (Ubuntu 17.5-1.pgdg24.04+1)
-- Dumped by pg_dump version 17.5 (Ubuntu 17.5-1.pgdg24.04+1)

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

--
-- Name: public; Type: SCHEMA; Schema: -; Owner: postgres
--

CREATE SCHEMA public;


ALTER SCHEMA public OWNER TO postgres;

--
-- Name: SCHEMA public; Type: COMMENT; Schema: -; Owner: postgres
--

COMMENT ON SCHEMA public IS 'standard public schema';


--
-- Name: process_store_app_audit(); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.process_store_app_audit() RETURNS trigger
LANGUAGE plpgsql
AS $$
    BEGIN
--
-- Create a row in store_app_audit to reflect the operation performed on store_apps,
-- making use of the special variable TG_OP to work out the operation.
--
IF (TG_OP = 'DELETE') THEN INSERT INTO logging.store_apps_audit
SELECT
    'D',
    now(),
    USER,
    OLD.id,
    OLD.store,
    OLD.store_id;
ELSIF (TG_OP = 'UPDATE') THEN
INSERT
    INTO
    logging.store_apps_audit
SELECT
    'U',
    now(),
    USER,
    NEW.id,
    NEW.store,
    NEW.store_id,
    NEW.crawl_result;
ELSIF (TG_OP = 'INSERT') THEN
INSERT
    INTO
    logging.store_apps_audit
SELECT
    'I',
    now(),
    USER,
    NEW.id,
    NEW.store,
    NEW.store_id;
END IF;

RETURN NULL;
-- result is ignored since this is an AFTER trigger
END;

$$;


ALTER FUNCTION public.process_store_app_audit() OWNER TO postgres;

--
-- Name: snapshot_apps(); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.snapshot_apps() RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN 
WITH alldata AS (
SELECT
    store,
    crawl_result,
    count(*) AS total_rows,
    avg(EXTRACT(DAY FROM now()-updated_at)) AS avg_days,
    max(EXTRACT(DAY FROM now()-updated_at)) AS max_days
FROM
    store_apps
GROUP BY
    store,
    crawl_result),
constb AS (
SELECT
    store,
    crawl_result,
    count(*) AS rows_older_than15
FROM
    store_apps sa
WHERE
    EXTRACT(DAY
FROM
    now()-updated_at) > 15
GROUP BY
    store,
    crawl_result
        )
INSERT
    INTO
    logging.store_apps_snapshot(store,
    crawl_result,
    total_rows,
    avg_days,
    max_days,
    rows_older_than15)
SELECT
    alldata.store,
    alldata.crawl_result,
    alldata.total_rows,
    alldata.avg_days,
    alldata.max_days,
    constb.rows_older_than15
FROM
    alldata
LEFT JOIN constb ON
    alldata.store = constb.store
    AND
    alldata.crawl_result = constb.crawl_result
    ;
END ;

$$;


ALTER FUNCTION public.snapshot_apps() OWNER TO postgres;

--
-- Name: snapshot_pub_domains(); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.snapshot_pub_domains() RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN 
WITH alldata AS (
SELECT
    crawl_result,
    count(*) AS total_rows,
    avg(EXTRACT(DAY FROM now()-updated_at)) AS avg_days,
    COALESCE(max(EXTRACT(DAY FROM now()-updated_at)), 0) AS max_days
FROM
    pub_domains
GROUP BY
    crawl_result
    ),
constb AS (
SELECT
    crawl_result,
    count(*) AS rows_older_than15
FROM
    pub_domains sa
WHERE
    EXTRACT(DAY
FROM
    now()-updated_at) > 15
GROUP BY
    crawl_result
        )
INSERT
    INTO
    logging.snapshot_pub_domains(
    crawl_result,
    total_rows,
    avg_days,
    max_days,
    rows_older_than15
    )
SELECT
    alldata.crawl_result,
    alldata.total_rows,
    alldata.avg_days,
    alldata.max_days,
    constb.rows_older_than15
FROM
    alldata
LEFT JOIN constb ON
    alldata.crawl_result = constb.crawl_result
    ;
END ;

$$;


ALTER FUNCTION public.snapshot_pub_domains() OWNER TO postgres;

--
-- Name: snapshot_store_apps(); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.snapshot_store_apps() RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN 
WITH alldata AS (
SELECT
    store,
    crawl_result,
    count(*) AS total_rows,
    avg(EXTRACT(DAY FROM now()-updated_at)) AS avg_days,
    COALESCE(max(EXTRACT(DAY FROM now()-updated_at)), 0) AS max_days
FROM
    store_apps
GROUP BY
    store,
    crawl_result),
constb AS (
SELECT
    store,
    crawl_result,
    count(*) AS rows_older_than15
FROM
    store_apps sa
WHERE
    EXTRACT(DAY
FROM
    now()-updated_at) > 15
GROUP BY
    store,
    crawl_result
        )
INSERT
    INTO
    logging.store_apps_snapshot(store,
    crawl_result,
    total_rows,
    avg_days,
    max_days,
    rows_older_than15)
SELECT
    alldata.store,
    alldata.crawl_result,
    alldata.total_rows,
    alldata.avg_days,
    alldata.max_days,
    constb.rows_older_than15
FROM
    alldata
LEFT JOIN constb ON
    alldata.store = constb.store
    AND
    alldata.crawl_result = constb.crawl_result
    ;
END ;

$$;


ALTER FUNCTION public.snapshot_store_apps() OWNER TO postgres;

--
-- Name: update_crawled_at(); Type: FUNCTION; Schema: public; Owner: james
--

CREATE FUNCTION public.update_crawled_at() RETURNS trigger
LANGUAGE plpgsql
AS $$ BEGIN NEW.crawled_at = now();
RETURN NEW;
END;
$$;


ALTER FUNCTION public.update_crawled_at() OWNER TO james;

--
-- Name: update_modified_column(); Type: FUNCTION; Schema: public; Owner: james
--

CREATE FUNCTION public.update_modified_column() RETURNS trigger
LANGUAGE plpgsql
AS $$ BEGIN NEW.updated_at = now();

RETURN NEW;

END;

$$;


ALTER FUNCTION public.update_modified_column() OWNER TO james;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: version_strings; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.version_strings (
    id integer NOT NULL,
    xml_path text NOT NULL,
    tag text,
    value_name text NOT NULL
);


ALTER TABLE public.version_strings OWNER TO postgres;

--
-- Name: store_apps; Type: TABLE; Schema: public; Owner: james
--

CREATE TABLE public.store_apps (
    id integer NOT NULL,
    developer integer,
    name character varying,
    store_id character varying NOT NULL,
    store integer NOT NULL,
    category character varying,
    rating double precision,
    review_count double precision,
    installs double precision,
    free boolean,
    price double precision,
    size text,
    minimum_android text,
    developer_email text,
    store_last_updated timestamp without time zone,
    content_rating text,
    ad_supported boolean,
    in_app_purchases boolean,
    editors_choice boolean,
    created_at timestamp without time zone DEFAULT timezone('utc'::text, now()),
    updated_at timestamp without time zone DEFAULT timezone('utc'::text, now()),
    crawl_result integer,
    icon_url_512 character varying,
    release_date date,
    rating_count integer,
    featured_image_url character varying,
    phone_image_url_1 character varying,
    phone_image_url_2 character varying,
    phone_image_url_3 character varying,
    tablet_image_url_1 character varying,
    tablet_image_url_2 character varying,
    tablet_image_url_3 character varying,
    textsearchable_index_col tsvector GENERATED ALWAYS AS (
        to_tsvector(
            'simple'::regconfig, (coalesce(name, ''::character varying))::text
        )
    ) STORED
);


ALTER TABLE public.store_apps OWNER TO james;

--
-- Name: version_codes; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.version_codes (
    id integer NOT NULL,
    store_app integer NOT NULL,
    version_code character varying NOT NULL,
    updated_at timestamp without time zone DEFAULT timezone('utc'::text, now()),
    crawl_result smallint NOT NULL,
    apk_hash character varying(32)
);


ALTER TABLE public.version_codes OWNER TO postgres;

--
-- Name: version_details_map; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.version_details_map (
    id bigint NOT NULL,
    version_code bigint,
    string_id integer
);


ALTER TABLE public.version_details_map OWNER TO postgres;

--
-- Name: app_urls_map; Type: TABLE; Schema: public; Owner: james
--

CREATE TABLE public.app_urls_map (
    id integer NOT NULL,
    store_app integer NOT NULL,
    pub_domain integer NOT NULL,
    created_at timestamp without time zone DEFAULT timezone('utc'::text, now()),
    updated_at timestamp without time zone DEFAULT timezone('utc'::text, now())
);


ALTER TABLE public.app_urls_map OWNER TO james;

--
-- Name: category_mapping; Type: MATERIALIZED VIEW; Schema: public; Owner: postgres
--

CREATE MATERIALIZED VIEW public.category_mapping AS
SELECT DISTINCT
    original_category,
    CASE
        WHEN
            (
                mapped_category
                = any(
                    ARRAY[
                        'action'::text,
                        'casual'::text,
                        'adventure'::text,
                        'arcade'::text,
                        'board'::text,
                        'card'::text,
                        'casino'::text,
                        'puzzle'::text,
                        'racing'::text,
                        'simulation'::text,
                        'strategy'::text,
                        'trivia'::text,
                        'word'::text
                    ]
                )
            )
            THEN ('game_'::text || mapped_category)
        WHEN (mapped_category = 'news_and_magazines'::text) THEN 'news'::text
        WHEN (mapped_category = 'educational'::text) THEN 'education'::text
        WHEN (mapped_category = 'book'::text) THEN 'books_and_reference'::text
        WHEN
            (mapped_category = 'navigation'::text)
            THEN 'maps_and_navigation'::text
        WHEN (mapped_category = 'music'::text) THEN 'music_and_audio'::text
        WHEN
            (mapped_category = 'photography'::text)
            THEN 'photo_and_video'::text
        WHEN
            (mapped_category = 'reference'::text)
            THEN 'books_and_reference'::text
        WHEN
            (mapped_category = 'role playing'::text)
            THEN 'game_role_playing'::text
        WHEN (mapped_category = 'social'::text) THEN 'social networking'::text
        WHEN (mapped_category = 'travel'::text) THEN 'travel_and_local'::text
        WHEN (mapped_category = 'utilities'::text) THEN 'tools'::text
        WHEN
            (mapped_category = 'video players_and_editors'::text)
            THEN 'video_players'::text
        WHEN
            (mapped_category = 'graphics_and_design'::text)
            THEN 'art_and_design'::text
        WHEN (mapped_category = 'parenting'::text) THEN 'family'::text
        WHEN (mapped_category IS null) THEN 'N/A'::text
        ELSE mapped_category
    END AS mapped_category
FROM (SELECT DISTINCT
    store_apps.category AS original_category,
    regexp_replace(
        lower((store_apps.category)::text), ' & '::text, '_and_'::text
    ) AS mapped_category
FROM public.store_apps) AS sub
WITH NO DATA;


ALTER MATERIALIZED VIEW public.category_mapping OWNER TO postgres;

--
-- Name: creative_assets; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.creative_assets (
    id integer NOT NULL,
    store_app_id integer NOT NULL,
    md5_hash character varying NOT NULL,
    file_extension character varying NOT NULL,
    phash character varying
);


ALTER TABLE public.creative_assets OWNER TO postgres;

--
-- Name: creative_records; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.creative_records (
    id integer NOT NULL,
    creative_initial_domain_id integer NOT NULL,
    creative_host_domain_id integer NOT NULL,
    run_id integer NOT NULL,
    store_app_pub_id integer NOT NULL,
    creative_asset_id integer NOT NULL,
    updated_at timestamp with time zone DEFAULT now(),
    mmp_domain_id integer,
    mmp_urls text [],
    additional_ad_domain_ids integer []
);


ALTER TABLE public.creative_records OWNER TO postgres;

--
-- Name: developers; Type: TABLE; Schema: public; Owner: james
--

CREATE TABLE public.developers (
    id integer NOT NULL,
    store integer NOT NULL,
    name character varying,
    developer_id character varying NOT NULL,
    created_at timestamp without time zone DEFAULT timezone('utc'::text, now()),
    updated_at timestamp without time zone DEFAULT timezone('utc'::text, now()),
    textsearchable_index_col tsvector GENERATED ALWAYS AS (
        to_tsvector(
            'simple'::regconfig, (coalesce(name, ''::character varying))::text
        )
    ) STORED
);


ALTER TABLE public.developers OWNER TO james;

--
-- Name: pub_domains; Type: TABLE; Schema: public; Owner: james
--

CREATE TABLE public.pub_domains (
    id integer NOT NULL,
    url character varying NOT NULL,
    created_at timestamp without time zone DEFAULT timezone('utc'::text, now()),
    updated_at timestamp without time zone DEFAULT timezone('utc'::text, now()),
    crawl_result integer,
    crawled_at timestamp without time zone
);


ALTER TABLE public.pub_domains OWNER TO james;

--
-- Name: store_apps_country_history; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.store_apps_country_history (
    store_app integer NOT NULL,
    review_count double precision,
    rating double precision,
    crawled_date date,
    rating_count integer,
    histogram bigint [] DEFAULT ARRAY[]::integer [],
    installs bigint,
    id integer NOT NULL,
    country_id smallint
);


ALTER TABLE public.store_apps_country_history OWNER TO postgres;

--
-- Name: store_apps_history_weekly; Type: MATERIALIZED VIEW; Schema: public; Owner: postgres
--

CREATE MATERIALIZED VIEW public.store_apps_history_weekly AS
WITH date_diffs AS (
    SELECT
        sach.store_app,
        sach.country_id,
        sach.crawled_date,
        sach.installs,
        sach.rating_count,
        max(sach.crawled_date)
            OVER (PARTITION BY sach.store_app, sach.country_id)
            AS last_date,
        (
            sach.installs
            - lead(sach.installs)
                OVER (
                    PARTITION BY sach.store_app, sach.country_id
                    ORDER BY sach.crawled_date DESC
                )
        ) AS installs_diff,
        (
            sach.rating_count
            - lead(sach.rating_count)
                OVER (
                    PARTITION BY sach.store_app, sach.country_id
                    ORDER BY sach.crawled_date DESC
                )
        ) AS rating_count_diff,
        (
            sach.crawled_date
            - lead(sach.crawled_date)
                OVER (
                    PARTITION BY sach.store_app, sach.country_id
                    ORDER BY sach.crawled_date DESC
                )
        ) AS days_diff
    FROM public.store_apps_country_history AS sach
    WHERE ((sach.store_app IN (
        SELECT sa.id
        FROM public.store_apps AS sa
        WHERE (sa.crawl_result = 1)
    )) AND (sach.crawled_date > (current_date - '375 days'::interval
    )))
), weekly_totals AS (
    SELECT
        (
            date_trunc(
                'week'::text,
                (date_diffs.crawled_date)::timestamp with time zone
            )
        )::date AS week_start,
        date_diffs.store_app,
        date_diffs.country_id,
        sum(date_diffs.installs_diff) AS installs_diff,
        sum(date_diffs.rating_count_diff) AS rating_count_diff,
        sum(date_diffs.days_diff) AS days_diff
    FROM date_diffs
    GROUP BY
        (
            (
                date_trunc(
                    'week'::text,
                    (date_diffs.crawled_date)::timestamp with time zone
                )
            )::date
        ),
        date_diffs.store_app,
        date_diffs.country_id
)
SELECT
    week_start,
    store_app,
    country_id,
    installs_diff,
    rating_count_diff
FROM weekly_totals
ORDER BY week_start DESC, store_app ASC, country_id ASC
WITH NO DATA;


ALTER MATERIALIZED VIEW public.store_apps_history_weekly OWNER TO postgres;

--
-- Name: store_app_z_scores; Type: MATERIALIZED VIEW; Schema: public; Owner: postgres
--

CREATE MATERIALIZED VIEW public.store_app_z_scores AS
WITH latest_week AS (
    SELECT max(store_apps_history_weekly.week_start) AS max_week
    FROM public.store_apps_history_weekly
), latest_week_per_app AS (
    SELECT
        store_apps_history_weekly.store_app,
        max(store_apps_history_weekly.week_start) AS app_max_week
    FROM public.store_apps_history_weekly
    WHERE (store_apps_history_weekly.country_id = 840)
    GROUP BY store_apps_history_weekly.store_app
), baseline_period AS (
    SELECT
        store_apps_history_weekly.store_app,
        avg(store_apps_history_weekly.installs_diff) AS avg_installs_diff,
        stddev(store_apps_history_weekly.installs_diff) AS stddev_installs_diff,
        avg(store_apps_history_weekly.rating_count_diff) AS avg_rating_diff,
        stddev(store_apps_history_weekly.rating_count_diff)
            AS stddev_rating_diff
    FROM public.store_apps_history_weekly,
        latest_week
    WHERE
        (
            (
                (
                    store_apps_history_weekly.week_start
                    >= (latest_week.max_week - '84 days'::interval)
                )
                AND (
                    store_apps_history_weekly.week_start
                    <= (latest_week.max_week - '35 days'::interval)
                )
            )
            AND (store_apps_history_weekly.country_id = 840)
        )
    GROUP BY store_apps_history_weekly.store_app
), recent_data AS (
    SELECT
        s.store_app,
        lw.max_week,
        s.week_start,
        s.installs_diff,
        s.rating_count_diff,
        CASE
            WHEN (s.week_start = lwpa.app_max_week) THEN 1
            ELSE 0
        END AS is_latest_week,
        CASE
            WHEN
                (s.week_start >= (lwpa.app_max_week - '14 days'::interval))
                THEN 1
            ELSE 0
        END AS in_2w_period,
        CASE
            WHEN
                (s.week_start >= (lwpa.app_max_week - '28 days'::interval))
                THEN 1
            ELSE 0
        END AS in_4w_period
    FROM ((
        public.store_apps_history_weekly s
        CROSS JOIN latest_week AS lw
    )
    INNER JOIN latest_week_per_app AS lwpa ON ((s.store_app = lwpa.store_app))
    )
    WHERE
        (
            (s.week_start >= (lw.max_week - '28 days'::interval))
            AND (s.country_id = 840)
        )
), aggregated_metrics AS (
    SELECT
        rd.store_app,
        rd.max_week AS latest_week,
        sum(
            CASE
                WHEN (rd.is_latest_week = 1) THEN rd.installs_diff
                ELSE (0)::numeric
            END
        ) AS installs_sum_1w,
        sum(
            CASE
                WHEN (rd.is_latest_week = 1) THEN rd.rating_count_diff
                ELSE (0)::bigint
            END
        ) AS ratings_sum_1w,
        (sum(
            CASE
                WHEN (rd.in_2w_period = 1) THEN rd.installs_diff
                ELSE (0)::numeric
            END) / (nullif(sum(
            CASE
                WHEN (rd.in_2w_period = 1) THEN 1
                ELSE 0
            END
        ), 0))::numeric) AS installs_avg_2w,
        (sum(
            CASE
                WHEN (rd.in_2w_period = 1) THEN rd.rating_count_diff
                ELSE (0)::bigint
            END) / (nullif(sum(
            CASE
                WHEN (rd.in_2w_period = 1) THEN 1
                ELSE 0
            END
        ), 0))::numeric) AS ratings_avg_2w,
        sum(
            CASE
                WHEN (rd.in_4w_period = 1) THEN rd.installs_diff
                ELSE (0)::numeric
            END
        ) AS installs_sum_4w,
        sum(
            CASE
                WHEN (rd.in_4w_period = 1) THEN rd.rating_count_diff
                ELSE (0)::bigint
            END
        ) AS ratings_sum_4w,
        (sum(
            CASE
                WHEN (rd.in_4w_period = 1) THEN rd.installs_diff
                ELSE (0)::numeric
            END) / (nullif(sum(
            CASE
                WHEN (rd.in_4w_period = 1) THEN 1
                ELSE 0
            END
        ), 0))::numeric) AS installs_avg_4w,
        (sum(
            CASE
                WHEN (rd.in_4w_period = 1) THEN rd.rating_count_diff
                ELSE (0)::bigint
            END) / (nullif(sum(
            CASE
                WHEN (rd.in_4w_period = 1) THEN 1
                ELSE 0
            END
        ), 0))::numeric) AS ratings_avg_4w
    FROM recent_data AS rd
    GROUP BY rd.store_app, rd.max_week
)
SELECT
    am.store_app,
    am.latest_week,
    am.installs_sum_1w,
    am.ratings_sum_1w,
    am.installs_avg_2w,
    am.ratings_avg_2w,
    am.installs_sum_4w,
    am.ratings_sum_4w,
    am.installs_avg_4w,
    am.ratings_avg_4w,
    (
        (am.installs_avg_2w - bp.avg_installs_diff)
        / nullif(bp.stddev_installs_diff, (0)::numeric)
    ) AS installs_z_score_2w,
    (
        (am.ratings_avg_2w - bp.avg_rating_diff)
        / nullif(bp.stddev_rating_diff, (0)::numeric)
    ) AS ratings_z_score_2w,
    (
        (am.installs_avg_4w - bp.avg_installs_diff)
        / nullif(bp.stddev_installs_diff, (0)::numeric)
    ) AS installs_z_score_4w,
    (
        (am.ratings_avg_4w - bp.avg_rating_diff)
        / nullif(bp.stddev_rating_diff, (0)::numeric)
    ) AS ratings_z_score_4w
FROM (
    aggregated_metrics AS am
    INNER JOIN baseline_period AS bp ON ((am.store_app = bp.store_app))
)
WITH NO DATA;


ALTER MATERIALIZED VIEW public.store_app_z_scores OWNER TO postgres;

--
-- Name: store_apps_descriptions; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.store_apps_descriptions (
    id integer NOT NULL,
    store_app integer NOT NULL,
    language_id integer NOT NULL,
    description text NOT NULL,
    description_short text NOT NULL,
    title text,
    updated_at timestamp without time zone DEFAULT now() NOT NULL,
    description_tsv tsvector
);


ALTER TABLE public.store_apps_descriptions OWNER TO postgres;

--
-- Name: version_code_api_scan_results; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.version_code_api_scan_results (
    id integer NOT NULL,
    version_code_id integer NOT NULL,
    run_name text,
    run_result smallint NOT NULL,
    run_at timestamp without time zone DEFAULT timezone(
        'utc'::text, now()
    ) NOT NULL
);


ALTER TABLE public.version_code_api_scan_results OWNER TO postgres;

--
-- Name: version_code_sdk_scan_results; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.version_code_sdk_scan_results (
    id integer NOT NULL,
    version_code_id integer,
    scan_result smallint NOT NULL,
    scanned_at timestamp without time zone DEFAULT timezone(
        'utc'::text, now()
    ) NOT NULL
);


ALTER TABLE public.version_code_sdk_scan_results OWNER TO postgres;

--
-- Name: ad_domains; Type: TABLE; Schema: public; Owner: james
--

CREATE TABLE public.ad_domains (
    id integer NOT NULL,
    domain character varying NOT NULL,
    created_at timestamp without time zone DEFAULT timezone('utc'::text, now()),
    updated_at timestamp without time zone DEFAULT timezone('utc'::text, now())
);


ALTER TABLE public.ad_domains OWNER TO james;

--
-- Name: app_ads_entrys; Type: TABLE; Schema: public; Owner: james
--

CREATE TABLE public.app_ads_entrys (
    id integer NOT NULL,
    ad_domain integer NOT NULL,
    publisher_id character varying NOT NULL,
    relationship character varying NOT NULL,
    certification_auth character varying,
    notes character varying,
    created_at timestamp without time zone DEFAULT timezone('utc'::text, now()),
    updated_at timestamp without time zone DEFAULT timezone('utc'::text, now())
);


ALTER TABLE public.app_ads_entrys OWNER TO james;

--
-- Name: app_ads_map; Type: TABLE; Schema: public; Owner: james
--

CREATE TABLE public.app_ads_map (
    id integer NOT NULL,
    pub_domain integer NOT NULL,
    app_ads_entry integer NOT NULL,
    created_at timestamp without time zone DEFAULT timezone('utc'::text, now()),
    updated_at timestamp without time zone DEFAULT timezone('utc'::text, now())
);


ALTER TABLE public.app_ads_map OWNER TO james;

--
-- Name: store_app_api_calls; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.store_app_api_calls (
    id integer NOT NULL,
    store_app integer NOT NULL,
    tld_url text NOT NULL,
    url text NOT NULL,
    host text NOT NULL,
    status_code integer NOT NULL,
    called_at timestamp without time zone NOT NULL,
    run_id integer NOT NULL,
    country_id integer,
    state_iso character varying(4),
    city_name character varying,
    org character varying
);


ALTER TABLE public.store_app_api_calls OWNER TO postgres;

--
-- Name: countries; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.countries (
    id integer NOT NULL,
    alpha2 character varying(2) NOT NULL,
    alpha3 character varying(3) NOT NULL,
    langcs character varying(45) NOT NULL,
    langde character varying(45) NOT NULL,
    langen character varying(45) NOT NULL,
    langes character varying(45) NOT NULL,
    langfr character varying(45) NOT NULL,
    langit character varying(45) NOT NULL,
    langnl character varying(45) NOT NULL
);


ALTER TABLE public.countries OWNER TO postgres;

--
-- Name: description_keywords; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.description_keywords (
    id integer NOT NULL,
    description_id integer NOT NULL,
    keyword_id integer NOT NULL,
    extracted_at timestamp without time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.description_keywords OWNER TO postgres;

--
-- Name: keywords; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.keywords (
    id integer NOT NULL,
    keyword_text character varying(255) NOT NULL
);


ALTER TABLE public.keywords OWNER TO postgres;

--
-- Name: ad_domains_id_seq; Type: SEQUENCE; Schema: public; Owner: james
--

ALTER TABLE public.ad_domains ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.ad_domains_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: version_manifests; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.version_manifests (
    id integer NOT NULL,
    version_code integer NOT NULL,
    manifest_string text NOT NULL
);


ALTER TABLE public.version_manifests OWNER TO postgres;

--
-- Name: ad_network_sdk_keys; Type: MATERIALIZED VIEW; Schema: public; Owner: postgres
--

CREATE MATERIALIZED VIEW public.ad_network_sdk_keys AS
WITH manifest_regex AS (
    SELECT
        vc.id AS version_code,
        vc.store_app,
        (
            regexp_match(
                vm.manifest_string,
                'applovin\.sdk\.key\"\ android\:value\=\"([^"]+)"'::text
            )
        )[1] AS applovin_sdk_key
    FROM (
        public.version_manifests AS vm
        LEFT JOIN public.version_codes AS vc ON ((vm.version_code = vc.id))
    )
), version_regex AS (
    SELECT
        vc.id AS version_code,
        vc.store_app,
        vs.value_name AS applovin_sdk_key
    FROM ((
        public.version_strings vs
        LEFT JOIN public.version_details_map AS vdm ON ((vs.id = vdm.string_id))
    )
    LEFT JOIN public.version_codes AS vc ON ((vdm.version_code = vc.id))
    )
    WHERE
        (
            (
                (vs.xml_path ~~* '%applovin%key%'::text)
                OR (vs.xml_path = 'applovin_settings.sdk_key'::text)
            )
            AND (length(vs.value_name) = 86)
        )
)
SELECT DISTINCT
    manifest_regex.store_app,
    manifest_regex.applovin_sdk_key
FROM manifest_regex
WHERE
    (
        (manifest_regex.applovin_sdk_key IS NOT null)
        AND (manifest_regex.applovin_sdk_key !~~ '@string%'::text)
    )
UNION
SELECT DISTINCT
    version_regex.store_app,
    version_regex.applovin_sdk_key
FROM version_regex
WITH NO DATA;


ALTER MATERIALIZED VIEW public.ad_network_sdk_keys OWNER TO postgres;

--
-- Name: app_ads_entrys_id_seq; Type: SEQUENCE; Schema: public; Owner: james
--

ALTER TABLE public.app_ads_entrys ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.app_ads_entrys_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: app_ads_map_id_seq; Type: SEQUENCE; Schema: public; Owner: james
--

ALTER TABLE public.app_ads_map ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.app_ads_map_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: app_keyword_rankings; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.app_keyword_rankings (
    id integer NOT NULL,
    crawled_date date NOT NULL,
    country smallint NOT NULL,
    lang smallint NOT NULL,
    keyword integer NOT NULL,
    rank smallint NOT NULL,
    store_app integer NOT NULL
);


ALTER TABLE public.app_keyword_rankings OWNER TO postgres;

--
-- Name: app_keyword_rankings_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.app_keyword_rankings_id_seq
AS integer
START WITH 1
INCREMENT BY 1
NO MINVALUE
NO MAXVALUE
CACHE 1;


ALTER SEQUENCE public.app_keyword_rankings_id_seq OWNER TO postgres;

--
-- Name: app_keyword_rankings_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.app_keyword_rankings_id_seq OWNED BY public.app_keyword_rankings.id;


--
-- Name: app_urls_map_id_seq; Type: SEQUENCE; Schema: public; Owner: james
--

ALTER TABLE public.app_urls_map ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.app_urls_map_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: apps_new_monthly; Type: MATERIALIZED VIEW; Schema: public; Owner: postgres
--

CREATE MATERIALIZED VIEW public.apps_new_monthly AS
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
    WHERE
        (
            (sa.release_date >= (current_date - '30 days'::interval))
            AND (sa.crawl_result = 1)
        )
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
WHERE (rn <= 100)
WITH NO DATA;


ALTER MATERIALIZED VIEW public.apps_new_monthly OWNER TO postgres;

--
-- Name: apps_new_weekly; Type: MATERIALIZED VIEW; Schema: public; Owner: postgres
--

CREATE MATERIALIZED VIEW public.apps_new_weekly AS
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
    WHERE
        (
            (sa.release_date >= (current_date - '7 days'::interval))
            AND (sa.crawl_result = 1)
        )
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
WHERE (rn <= 100)
WITH NO DATA;


ALTER MATERIALIZED VIEW public.apps_new_weekly OWNER TO postgres;

--
-- Name: apps_new_yearly; Type: MATERIALIZED VIEW; Schema: public; Owner: postgres
--

CREATE MATERIALIZED VIEW public.apps_new_yearly AS
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
    WHERE
        (
            (sa.release_date >= (current_date - '365 days'::interval))
            AND (sa.crawl_result = 1)
        )
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
WHERE (rn <= 100)
WITH NO DATA;


ALTER MATERIALIZED VIEW public.apps_new_yearly OWNER TO postgres;

--
-- Name: audit_dates; Type: MATERIALIZED VIEW; Schema: public; Owner: postgres
--

CREATE MATERIALIZED VIEW public.audit_dates AS
WITH sa AS (
    SELECT
        (store_apps_audit.stamp)::date AS updated_date,
        'store_apps'::text AS table_name,
        count(*) AS updated_count
    FROM logging.store_apps_audit
    GROUP BY ((store_apps_audit.stamp)::date)
)
SELECT
    updated_date,
    table_name,
    updated_count
FROM sa
WITH NO DATA;


ALTER MATERIALIZED VIEW public.audit_dates OWNER TO postgres;

--
-- Name: crawl_results; Type: TABLE; Schema: public; Owner: james
--

CREATE TABLE public.crawl_results (
    id smallint NOT NULL,
    outcome character varying
);


ALTER TABLE public.crawl_results OWNER TO james;

--
-- Name: crawl_results_id_seq; Type: SEQUENCE; Schema: public; Owner: james
--

ALTER TABLE public.crawl_results ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.crawl_results_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: creative_assets_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.creative_assets_id_seq
AS integer
START WITH 1
INCREMENT BY 1
NO MINVALUE
NO MAXVALUE
CACHE 1;


ALTER SEQUENCE public.creative_assets_id_seq OWNER TO postgres;

--
-- Name: creative_assets_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.creative_assets_id_seq OWNED BY public.creative_assets.id;


--
-- Name: creative_records_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.creative_records_id_seq
AS integer
START WITH 1
INCREMENT BY 1
NO MINVALUE
NO MAXVALUE
CACHE 1;


ALTER SEQUENCE public.creative_records_id_seq OWNER TO postgres;

--
-- Name: creative_records_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.creative_records_id_seq OWNED BY public.creative_records.id;


--
-- Name: description_keywords_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.description_keywords_id_seq
AS integer
START WITH 1
INCREMENT BY 1
NO MINVALUE
NO MAXVALUE
CACHE 1;


ALTER SEQUENCE public.description_keywords_id_seq OWNER TO postgres;

--
-- Name: description_keywords_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.description_keywords_id_seq OWNED BY public.description_keywords.id;


--
-- Name: developer_store_apps; Type: MATERIALIZED VIEW; Schema: public; Owner: postgres
--

CREATE MATERIALIZED VIEW public.developer_store_apps AS
WITH developer_domain_ids AS (
    SELECT DISTINCT pd_1.id AS domain_id
    FROM (((
        public.app_urls_map aum_1
        LEFT JOIN public.pub_domains AS pd_1 ON ((aum_1.pub_domain = pd_1.id))
    )
    LEFT JOIN public.store_apps AS sa_1 ON ((aum_1.store_app = sa_1.id))
    )
    LEFT JOIN public.developers AS d_1 ON ((sa_1.developer = d_1.id))
    )
)
SELECT
    sa.store,
    sa.store_id,
    sa.name,
    sa.icon_url_512,
    sa.installs,
    sa.rating,
    sa.rating_count,
    sa.review_count,
    d.name AS developer_name,
    pd.url AS developer_url,
    d.store AS developer_store,
    pd.id AS domain_id,
    d.developer_id
FROM (((
    public.store_apps sa
    LEFT JOIN public.developers AS d ON ((sa.developer = d.id))
)
LEFT JOIN public.app_urls_map AS aum ON ((sa.id = aum.store_app))
)
LEFT JOIN public.pub_domains AS pd ON ((aum.pub_domain = pd.id))
)
ORDER BY sa.installs DESC NULLS LAST, sa.rating_count DESC NULLS LAST
WITH NO DATA;


ALTER MATERIALIZED VIEW public.developer_store_apps OWNER TO postgres;

--
-- Name: developers_id_seq; Type: SEQUENCE; Schema: public; Owner: james
--

ALTER TABLE public.developers ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.developers_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: keywords_base; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.keywords_base (
    keyword_id bigint
);


ALTER TABLE public.keywords_base OWNER TO postgres;

--
-- Name: keywords_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.keywords_id_seq
AS integer
START WITH 1
INCREMENT BY 1
NO MINVALUE
NO MAXVALUE
CACHE 1;


ALTER SEQUENCE public.keywords_id_seq OWNER TO postgres;

--
-- Name: keywords_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.keywords_id_seq OWNED BY public.keywords.id;


--
-- Name: languages; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.languages (
    id smallint NOT NULL,
    language_slug character varying(5)
);


ALTER TABLE public.languages OWNER TO postgres;

--
-- Name: languages_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.languages_id_seq
AS smallint
START WITH 1
INCREMENT BY 1
NO MINVALUE
NO MAXVALUE
CACHE 1;


ALTER SEQUENCE public.languages_id_seq OWNER TO postgres;

--
-- Name: languages_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.languages_id_seq OWNED BY public.languages.id;


--
-- Name: mv_app_categories; Type: MATERIALIZED VIEW; Schema: public; Owner: postgres
--

CREATE MATERIALIZED VIEW public.mv_app_categories AS
SELECT
    sa.store,
    cm.mapped_category AS category,
    count(*) AS app_count
FROM (
    public.store_apps AS sa
    INNER JOIN
        public.category_mapping AS cm
        ON (((sa.category)::text = (cm.original_category)::text))
)
WHERE ((sa.crawl_result = 1) AND (sa.category IS NOT null))
GROUP BY sa.store, cm.mapped_category
ORDER BY sa.store, cm.mapped_category
WITH NO DATA;


ALTER MATERIALIZED VIEW public.mv_app_categories OWNER TO postgres;

--
-- Name: platforms; Type: TABLE; Schema: public; Owner: james
--

CREATE TABLE public.platforms (
    id smallint NOT NULL,
    name character varying NOT NULL
);


ALTER TABLE public.platforms OWNER TO james;

--
-- Name: newtable_id_seq; Type: SEQUENCE; Schema: public; Owner: james
--

CREATE SEQUENCE public.newtable_id_seq
AS integer
START WITH 1
INCREMENT BY 1
NO MINVALUE
NO MAXVALUE
CACHE 1;


ALTER SEQUENCE public.newtable_id_seq OWNER TO james;

--
-- Name: newtable_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: james
--

ALTER SEQUENCE public.newtable_id_seq OWNED BY public.platforms.id;


--
-- Name: pub_domains_id_seq; Type: SEQUENCE; Schema: public; Owner: james
--

ALTER TABLE public.pub_domains ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.pub_domains_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: store_app_api_calls_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.store_app_api_calls_id_seq
AS integer
START WITH 1
INCREMENT BY 1
NO MINVALUE
NO MAXVALUE
CACHE 1;


ALTER SEQUENCE public.store_app_api_calls_id_seq OWNER TO postgres;

--
-- Name: store_app_api_calls_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.store_app_api_calls_id_seq OWNED BY public.store_app_api_calls.id;


--
-- Name: store_apps_country_history_idx_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

ALTER TABLE public.store_apps_country_history ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.store_apps_country_history_idx_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: store_apps_created_at; Type: MATERIALIZED VIEW; Schema: public; Owner: postgres
--

CREATE MATERIALIZED VIEW public.store_apps_created_at AS
WITH my_dates AS (
    SELECT
        num_series.store,
        (
            generate_series(
                (current_date - '365 days'::interval),
                (current_date)::timestamp without time zone,
                '1 day'::interval
            )
        )::date AS date
    FROM generate_series(1, 2, 1) AS num_series (store)
), created_dates AS (
    SELECT
        sa.store,
        (sa.created_at)::date AS created_date,
        sas.crawl_source,
        count(*) AS created_count
    FROM (
        public.store_apps AS sa
        LEFT JOIN
            logging.store_app_sources AS sas
            ON (((sa.id = sas.store_app) AND (sa.store = sas.store)))
    )
    WHERE (sa.created_at >= (current_date - '365 days'::interval))
    GROUP BY sa.store, ((sa.created_at)::date), sas.crawl_source
)
SELECT
    my_dates.store,
    my_dates.date,
    created_dates.crawl_source,
    created_dates.created_count
FROM (
    my_dates
    LEFT JOIN
        created_dates
        ON
            (
                (
                    (my_dates.date = created_dates.created_date)
                    AND (my_dates.store = created_dates.store)
                )
            )
)
WITH NO DATA;


ALTER MATERIALIZED VIEW public.store_apps_created_at OWNER TO postgres;

--
-- Name: store_apps_descriptions_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.store_apps_descriptions_id_seq
AS integer
START WITH 1
INCREMENT BY 1
NO MINVALUE
NO MAXVALUE
CACHE 1;


ALTER SEQUENCE public.store_apps_descriptions_id_seq OWNER TO postgres;

--
-- Name: store_apps_descriptions_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.store_apps_descriptions_id_seq OWNED BY public.store_apps_descriptions.id;


--
-- Name: store_apps_id_seq; Type: SEQUENCE; Schema: public; Owner: james
--

ALTER TABLE public.store_apps ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.store_apps_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: store_apps_in_latest_rankings; Type: MATERIALIZED VIEW; Schema: public; Owner: postgres
--

CREATE MATERIALIZED VIEW public.store_apps_in_latest_rankings AS
WITH growth_apps AS (
    SELECT
        sa.id AS store_app,
        sa.store,
        sa.store_last_updated,
        sa.name,
        sa.installs,
        sa.rating_count,
        sa.store_id
    FROM (
        frontend.store_apps_z_scores AS saz
        LEFT JOIN
            public.store_apps AS sa
            ON (((saz.store_id)::text = (sa.store_id)::text))
    )
    WHERE sa.free
    ORDER BY coalesce(saz.installs_z_score_2w, saz.ratings_z_score_2w) DESC
    LIMIT 500
), ranked_apps AS (
    SELECT DISTINCT ON (ar.store_app)
        ar.store_app,
        sa.store,
        sa.store_last_updated,
        sa.name,
        sa.installs,
        sa.rating_count,
        sa.store_id
    FROM ((
        frontend.store_app_ranks_weekly ar
        LEFT JOIN public.store_apps AS sa ON ((ar.store_app = sa.id))
    )
    LEFT JOIN public.countries AS c ON ((ar.country = c.id))
    )
    WHERE
        (
            sa.free
            AND (ar.store_collection = any(ARRAY[1, 3, 4, 6]))
            AND (ar.crawled_date > (current_date - '15 days'::interval))
            AND (
                (c.alpha2)::text
                = any(
                    ARRAY[
                        ('US'::character varying)::text,
                        ('GB'::character varying)::text,
                        ('CA'::character varying)::text,
                        ('AR'::character varying)::text,
                        ('CN'::character varying)::text,
                        ('DE'::character varying)::text,
                        ('ID'::character varying)::text,
                        ('IN'::character varying)::text,
                        ('JP'::character varying)::text,
                        ('FR'::character varying)::text,
                        ('BR'::character varying)::text,
                        ('MX'::character varying)::text,
                        ('KR'::character varying)::text,
                        ('RU'::character varying)::text
                    ]
                )
            )
            AND (ar.rank < 150)
        )
)
SELECT
    growth_apps.store_app,
    growth_apps.store,
    growth_apps.store_last_updated,
    growth_apps.name,
    growth_apps.installs,
    growth_apps.rating_count,
    growth_apps.store_id
FROM growth_apps
UNION
SELECT
    ranked_apps.store_app,
    ranked_apps.store,
    ranked_apps.store_last_updated,
    ranked_apps.name,
    ranked_apps.installs,
    ranked_apps.rating_count,
    ranked_apps.store_id
FROM ranked_apps
WITH NO DATA;


ALTER MATERIALIZED VIEW public.store_apps_in_latest_rankings OWNER TO postgres;

--
-- Name: store_apps_updated_at; Type: MATERIALIZED VIEW; Schema: public; Owner: postgres
--

CREATE MATERIALIZED VIEW public.store_apps_updated_at AS
WITH my_dates AS (
    SELECT
        num_series.store,
        (
            generate_series(
                (current_date - '365 days'::interval),
                (current_date)::timestamp without time zone,
                '1 day'::interval
            )
        )::date AS date
    FROM generate_series(1, 2, 1) AS num_series (store)
), updated_dates AS (
    SELECT
        store_apps.store,
        (store_apps.updated_at)::date AS last_updated_date,
        count(*) AS last_updated_count
    FROM public.store_apps
    WHERE (store_apps.updated_at >= (current_date - '365 days'::interval))
    GROUP BY store_apps.store, ((store_apps.updated_at)::date)
)
SELECT
    my_dates.store,
    my_dates.date,
    updated_dates.last_updated_count,
    audit_dates.updated_count
FROM ((
    my_dates
    LEFT JOIN
        updated_dates
        ON
            (
                (
                    (my_dates.date = updated_dates.last_updated_date)
                    AND (my_dates.store = updated_dates.store)
                )
            )
)
LEFT JOIN public.audit_dates ON ((my_dates.date = audit_dates.updated_date)))
ORDER BY my_dates.date DESC
WITH NO DATA;


ALTER MATERIALIZED VIEW public.store_apps_updated_at OWNER TO postgres;

--
-- Name: store_categories; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.store_categories (
    id smallint NOT NULL,
    store smallint NOT NULL,
    category character varying NOT NULL
);


ALTER TABLE public.store_categories OWNER TO postgres;

--
-- Name: store_categories_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

ALTER TABLE public.store_categories ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.store_categories_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: store_collections; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.store_collections (
    id smallint NOT NULL,
    store smallint NOT NULL,
    collection character varying NOT NULL
);


ALTER TABLE public.store_collections OWNER TO postgres;

--
-- Name: store_collections_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

ALTER TABLE public.store_collections ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.store_collections_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: stores; Type: TABLE; Schema: public; Owner: james
--

CREATE TABLE public.stores (
    id smallint NOT NULL,
    name character varying NOT NULL,
    platform smallint
);


ALTER TABLE public.stores OWNER TO james;

--
-- Name: stores_column1_seq; Type: SEQUENCE; Schema: public; Owner: james
--

CREATE SEQUENCE public.stores_column1_seq
AS integer
START WITH 1
INCREMENT BY 1
NO MINVALUE
NO MAXVALUE
CACHE 1;


ALTER SEQUENCE public.stores_column1_seq OWNER TO james;

--
-- Name: stores_column1_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: james
--

ALTER SEQUENCE public.stores_column1_seq OWNED BY public.stores.id;


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
-- Name: total_count_overview; Type: MATERIALIZED VIEW; Schema: public; Owner: postgres
--

CREATE MATERIALIZED VIEW public.total_count_overview AS
WITH app_count AS (
    SELECT
        count(
            CASE
                WHEN (sa.store = 1) THEN 1
                ELSE null::integer
            END
        ) AS android_apps,
        count(
            CASE
                WHEN (sa.store = 2) THEN 1
                ELSE null::integer
            END
        ) AS ios_apps,
        count(
            CASE
                WHEN ((sa.store = 1) AND (sa.crawl_result = 1)) THEN 1
                ELSE null::integer
            END
        ) AS success_android_apps,
        count(
            CASE
                WHEN ((sa.store = 2) AND (sa.crawl_result = 1)) THEN 1
                ELSE null::integer
            END
        ) AS success_ios_apps,
        count(
            CASE
                WHEN
                    (
                        (sa.store = 1)
                        AND (
                            sa.updated_at >= (current_date - '7 days'::interval)
                        )
                    )
                    THEN 1
                ELSE null::integer
            END
        ) AS weekly_scanned_android_apps,
        count(
            CASE
                WHEN
                    (
                        (sa.store = 2)
                        AND (
                            sa.updated_at >= (current_date - '7 days'::interval)
                        )
                    )
                    THEN 1
                ELSE null::integer
            END
        ) AS weekly_scanned_ios_apps,
        count(
            CASE
                WHEN
                    (
                        (sa.store = 1)
                        AND (sa.crawl_result = 1)
                        AND (
                            sa.updated_at >= (current_date - '7 days'::interval)
                        )
                    )
                    THEN 1
                ELSE null::integer
            END
        ) AS weekly_success_scanned_android_apps,
        count(
            CASE
                WHEN
                    (
                        (sa.store = 2)
                        AND (sa.crawl_result = 1)
                        AND (
                            sa.updated_at >= (current_date - '7 days'::interval)
                        )
                    )
                    THEN 1
                ELSE null::integer
            END
        ) AS weekly_success_scanned_ios_apps
    FROM public.store_apps AS sa
), sdk_app_count AS (
    SELECT
        count(
            DISTINCT
            CASE
                WHEN (sa.store = 1) THEN vc.store_app
                ELSE null::integer
            END
        ) AS sdk_android_apps,
        count(
            DISTINCT
            CASE
                WHEN (sa.store = 2) THEN vc.store_app
                ELSE null::integer
            END
        ) AS sdk_ios_apps,
        count(
            DISTINCT
            CASE
                WHEN
                    ((sa.store = 1) AND (vc.crawl_result = 1))
                    THEN vc.store_app
                ELSE null::integer
            END
        ) AS sdk_success_android_apps,
        count(
            DISTINCT
            CASE
                WHEN
                    ((sa.store = 2) AND (vc.crawl_result = 1))
                    THEN vc.store_app
                ELSE null::integer
            END
        ) AS sdk_success_ios_apps,
        count(
            DISTINCT
            CASE
                WHEN
                    (
                        (sa.store = 1)
                        AND (
                            vc.updated_at >= (current_date - '7 days'::interval)
                        )
                        AND (vc.crawl_result = 1)
                    )
                    THEN vc.store_app
                ELSE null::integer
            END
        ) AS sdk_weekly_success_android_apps,
        count(
            DISTINCT
            CASE
                WHEN
                    (
                        (sa.store = 2)
                        AND (
                            vc.updated_at >= (current_date - '7 days'::interval)
                        )
                        AND (vc.crawl_result = 1)
                    )
                    THEN vc.store_app
                ELSE null::integer
            END
        ) AS sdk_weekly_success_ios_apps,
        count(
            DISTINCT
            CASE
                WHEN
                    (
                        (sa.store = 1)
                        AND (
                            vc.updated_at >= (current_date - '7 days'::interval)
                        )
                    )
                    THEN vc.store_app
                ELSE null::integer
            END
        ) AS sdk_weekly_android_apps,
        count(
            DISTINCT
            CASE
                WHEN
                    (
                        (sa.store = 2)
                        AND (
                            vc.updated_at >= (current_date - '7 days'::interval)
                        )
                    )
                    THEN vc.store_app
                ELSE null::integer
            END
        ) AS sdk_weekly_ios_apps
    FROM (
        public.version_codes AS vc
        LEFT JOIN public.store_apps AS sa ON ((vc.store_app = sa.id))
    )
), appads_url_count AS (
    SELECT
        count(DISTINCT pd.url) AS appads_urls,
        count(
            DISTINCT
            CASE
                WHEN (pd.crawl_result = 1) THEN pd.url
                ELSE null::character varying
            END
        ) AS appads_success_urls,
        count(
            DISTINCT
            CASE
                WHEN
                    (
                        (pd.crawl_result = 1)
                        AND (
                            pd.updated_at >= (current_date - '7 days'::interval)
                        )
                    )
                    THEN pd.url
                ELSE null::character varying
            END
        ) AS appads_weekly_success_urls,
        count(
            DISTINCT
            CASE
                WHEN
                    (pd.updated_at >= (current_date - '7 days'::interval))
                    THEN pd.url
                ELSE null::character varying
            END
        ) AS appads_weekly_urls
    FROM public.pub_domains AS pd
)
SELECT
    app_count.android_apps,
    app_count.ios_apps,
    app_count.success_android_apps,
    app_count.success_ios_apps,
    app_count.weekly_scanned_android_apps,
    app_count.weekly_scanned_ios_apps,
    app_count.weekly_success_scanned_android_apps,
    app_count.weekly_success_scanned_ios_apps,
    sdk_app_count.sdk_android_apps,
    sdk_app_count.sdk_ios_apps,
    sdk_app_count.sdk_success_android_apps,
    sdk_app_count.sdk_success_ios_apps,
    sdk_app_count.sdk_weekly_success_android_apps,
    sdk_app_count.sdk_weekly_success_ios_apps,
    sdk_app_count.sdk_weekly_android_apps,
    sdk_app_count.sdk_weekly_ios_apps,
    appads_url_count.appads_urls,
    appads_url_count.appads_success_urls,
    appads_url_count.appads_weekly_success_urls,
    appads_url_count.appads_weekly_urls,
    current_date AS on_date
FROM app_count,
    sdk_app_count,
    appads_url_count
WITH NO DATA;


ALTER MATERIALIZED VIEW public.total_count_overview OWNER TO postgres;

--
-- Name: user_requested_scan; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.user_requested_scan (
    id integer NOT NULL,
    store_id character varying NOT NULL,
    created_at timestamp without time zone DEFAULT current_timestamp
);


ALTER TABLE public.user_requested_scan OWNER TO postgres;

--
-- Name: user_requested_scan_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.user_requested_scan_id_seq
AS integer
START WITH 1
INCREMENT BY 1
NO MINVALUE
NO MAXVALUE
CACHE 1;


ALTER SEQUENCE public.user_requested_scan_id_seq OWNER TO postgres;

--
-- Name: user_requested_scan_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.user_requested_scan_id_seq OWNED BY public.user_requested_scan.id;


--
-- Name: version_code_api_scan_results_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.version_code_api_scan_results_id_seq
AS integer
START WITH 1
INCREMENT BY 1
NO MINVALUE
NO MAXVALUE
CACHE 1;


ALTER SEQUENCE public.version_code_api_scan_results_id_seq OWNER TO postgres;

--
-- Name: version_code_api_scan_results_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.version_code_api_scan_results_id_seq OWNED BY public.version_code_api_scan_results.id;


--
-- Name: version_code_sdk_scan_results_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.version_code_sdk_scan_results_id_seq
AS integer
START WITH 1
INCREMENT BY 1
NO MINVALUE
NO MAXVALUE
CACHE 1;


ALTER SEQUENCE public.version_code_sdk_scan_results_id_seq OWNER TO postgres;

--
-- Name: version_code_sdk_scan_results_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.version_code_sdk_scan_results_id_seq OWNED BY public.version_code_sdk_scan_results.id;


--
-- Name: version_codes_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

ALTER TABLE public.version_codes ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.version_codes_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: version_details_map_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

ALTER TABLE public.version_details_map ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.version_details_map_id_seq
    START WITH 49502675
    INCREMENT BY 1
    NO MINVALUE
    MAXVALUE 2147483647
    CACHE 1
);


--
-- Name: version_manifests_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

ALTER TABLE public.version_manifests ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.version_manifests_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: version_strings_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

ALTER TABLE public.version_strings ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.version_strings_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: app_keyword_rankings id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.app_keyword_rankings ALTER COLUMN id SET DEFAULT nextval(
    'public.app_keyword_rankings_id_seq'::regclass
);


--
-- Name: creative_assets id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.creative_assets ALTER COLUMN id SET DEFAULT nextval(
    'public.creative_assets_id_seq'::regclass
);


--
-- Name: creative_records id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.creative_records ALTER COLUMN id SET DEFAULT nextval(
    'public.creative_records_id_seq'::regclass
);


--
-- Name: description_keywords id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.description_keywords ALTER COLUMN id SET DEFAULT nextval(
    'public.description_keywords_id_seq'::regclass
);


--
-- Name: keywords id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.keywords ALTER COLUMN id SET DEFAULT nextval(
    'public.keywords_id_seq'::regclass
);


--
-- Name: languages id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.languages ALTER COLUMN id SET DEFAULT nextval(
    'public.languages_id_seq'::regclass
);


--
-- Name: platforms id; Type: DEFAULT; Schema: public; Owner: james
--

ALTER TABLE ONLY public.platforms ALTER COLUMN id SET DEFAULT nextval(
    'public.newtable_id_seq'::regclass
);


--
-- Name: store_app_api_calls id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.store_app_api_calls ALTER COLUMN id SET DEFAULT nextval(
    'public.store_app_api_calls_id_seq'::regclass
);


--
-- Name: store_apps_descriptions id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.store_apps_descriptions ALTER COLUMN id SET DEFAULT nextval(
    'public.store_apps_descriptions_id_seq'::regclass
);


--
-- Name: stores id; Type: DEFAULT; Schema: public; Owner: james
--

ALTER TABLE ONLY public.stores ALTER COLUMN id SET DEFAULT nextval(
    'public.stores_column1_seq'::regclass
);


--
-- Name: user_requested_scan id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.user_requested_scan ALTER COLUMN id SET DEFAULT nextval(
    'public.user_requested_scan_id_seq'::regclass
);


--
-- Name: version_code_api_scan_results id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.version_code_api_scan_results ALTER COLUMN id SET DEFAULT nextval(
    'public.version_code_api_scan_results_id_seq'::regclass
);


--
-- Name: version_code_sdk_scan_results id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.version_code_sdk_scan_results ALTER COLUMN id SET DEFAULT nextval(
    'public.version_code_sdk_scan_results_id_seq'::regclass
);


--
-- Name: SCHEMA public; Type: ACL; Schema: -; Owner: postgres
--

REVOKE USAGE ON SCHEMA public FROM public;
GRANT ALL ON SCHEMA public TO public;


--
-- PostgreSQL database dump complete
--
