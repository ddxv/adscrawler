--
-- PostgreSQL database dump
--

\restrict H8HMtkDkYggiiYeKcTWAfeZZbH0PdPaq1xAHjMcJbSMkJcdpS7FTANjZwyfksa7

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

--
-- Name: adtech; Type: SCHEMA; Schema: -; Owner: postgres
--

CREATE SCHEMA adtech;


ALTER SCHEMA adtech OWNER TO postgres;

--
-- Name: frontend; Type: SCHEMA; Schema: -; Owner: postgres
--

CREATE SCHEMA frontend;


ALTER SCHEMA frontend OWNER TO postgres;

--
-- Name: logging; Type: SCHEMA; Schema: -; Owner: postgres
--

CREATE SCHEMA logging;


ALTER SCHEMA logging OWNER TO postgres;

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
-- Name: extract_scheme(text); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.extract_scheme(url_text text) RETURNS text
    LANGUAGE plpgsql IMMUTABLE
    AS $$
BEGIN
    -- Extract scheme (everything before ://)
    RETURN LOWER(SUBSTRING(url_text FROM '^([^:]+)://'));
EXCEPTION WHEN OTHERS THEN
    -- Default to 'unknown' if parsing fails
    RETURN 'unknown';
END;
$$;


ALTER FUNCTION public.extract_scheme(url_text text) OWNER TO postgres;

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
-- Name: api_call_urls; Type: TABLE; Schema: adtech; Owner: postgres
--

CREATE TABLE adtech.api_call_urls (
    id integer NOT NULL,
    run_id integer NOT NULL,
    api_call_id integer,
    url_id integer NOT NULL,
    created_at timestamp with time zone DEFAULT now()
);


ALTER TABLE adtech.api_call_urls OWNER TO postgres;

--
-- Name: api_call_urls_id_seq; Type: SEQUENCE; Schema: adtech; Owner: postgres
--

CREATE SEQUENCE adtech.api_call_urls_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE adtech.api_call_urls_id_seq OWNER TO postgres;

--
-- Name: api_call_urls_id_seq; Type: SEQUENCE OWNED BY; Schema: adtech; Owner: postgres
--

ALTER SEQUENCE adtech.api_call_urls_id_seq OWNED BY adtech.api_call_urls.id;


--
-- Name: categories; Type: TABLE; Schema: adtech; Owner: postgres
--

CREATE TABLE adtech.categories (
    id integer NOT NULL,
    name character varying NOT NULL,
    url_slug character varying
);


ALTER TABLE adtech.categories OWNER TO postgres;

--
-- Name: categories_id_seq; Type: SEQUENCE; Schema: adtech; Owner: postgres
--

ALTER TABLE adtech.categories ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME adtech.categories_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: companies; Type: TABLE; Schema: adtech; Owner: postgres
--

CREATE TABLE adtech.companies (
    id integer NOT NULL,
    name character varying NOT NULL,
    parent_company_id integer,
    description text,
    domain_id integer NOT NULL,
    logo_url text,
    linkedin_url character varying
);


ALTER TABLE adtech.companies OWNER TO postgres;

--
-- Name: company_developers; Type: TABLE; Schema: adtech; Owner: postgres
--

CREATE TABLE adtech.company_developers (
    company_id integer NOT NULL,
    developer_id integer NOT NULL
);


ALTER TABLE adtech.company_developers OWNER TO postgres;

--
-- Name: company_domain_mapping; Type: TABLE; Schema: adtech; Owner: postgres
--

CREATE TABLE adtech.company_domain_mapping (
    company_id integer NOT NULL,
    domain_id integer NOT NULL
);


ALTER TABLE adtech.company_domain_mapping OWNER TO postgres;

--
-- Name: sdk_mediation_patterns; Type: TABLE; Schema: adtech; Owner: postgres
--

CREATE TABLE adtech.sdk_mediation_patterns (
    sdk_id integer NOT NULL,
    mediation_pattern character varying(255) NOT NULL
);


ALTER TABLE adtech.sdk_mediation_patterns OWNER TO postgres;

--
-- Name: sdk_packages; Type: TABLE; Schema: adtech; Owner: postgres
--

CREATE TABLE adtech.sdk_packages (
    id integer NOT NULL,
    package_pattern character varying NOT NULL,
    sdk_id integer
);


ALTER TABLE adtech.sdk_packages OWNER TO postgres;

--
-- Name: sdk_paths; Type: TABLE; Schema: adtech; Owner: postgres
--

CREATE TABLE adtech.sdk_paths (
    id integer NOT NULL,
    path_pattern character varying NOT NULL,
    sdk_id integer
);


ALTER TABLE adtech.sdk_paths OWNER TO postgres;

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
-- Name: sdk_strings; Type: MATERIALIZED VIEW; Schema: adtech; Owner: postgres
--

CREATE MATERIALIZED VIEW adtech.sdk_strings AS
 WITH matched_value_patterns AS (
         SELECT DISTINCT lower(vd.value_name) AS value_name_lower,
            sp.sdk_id
           FROM (public.version_strings vd
             JOIN adtech.sdk_packages sp ON ((lower(vd.value_name) ~~ (lower((sp.package_pattern)::text) || '%'::text))))
        ), matched_path_patterns AS (
         SELECT DISTINCT lower(vd.xml_path) AS xml_path_lower,
            ptm.sdk_id
           FROM (public.version_strings vd
             JOIN adtech.sdk_paths ptm ON ((lower(vd.xml_path) = lower((ptm.path_pattern)::text))))
        ), mediation_strings AS (
         SELECT vs.id AS version_string_id,
            cmp.sdk_id,
            lower(vs.value_name) AS value_name_lower
           FROM (public.version_strings vs
             JOIN adtech.sdk_mediation_patterns cmp ON ((lower(vs.value_name) ~~ (lower(concat((cmp.mediation_pattern)::text, '.')) || '%'::text))))
        )
 SELECT vs.id AS version_string_id,
    mp.sdk_id
   FROM (matched_value_patterns mp
     JOIN public.version_strings vs ON ((lower(vs.value_name) = mp.value_name_lower)))
UNION
 SELECT vs.id AS version_string_id,
    mp.sdk_id
   FROM (matched_path_patterns mp
     JOIN public.version_strings vs ON ((lower(vs.xml_path) = mp.xml_path_lower)))
UNION
 SELECT vs.id AS version_string_id,
    ms.sdk_id
   FROM (mediation_strings ms
     JOIN public.version_strings vs ON ((lower(vs.value_name) = ms.value_name_lower)))
  WITH NO DATA;


ALTER MATERIALIZED VIEW adtech.sdk_strings OWNER TO postgres;

--
-- Name: sdks; Type: TABLE; Schema: adtech; Owner: postgres
--

CREATE TABLE adtech.sdks (
    id integer NOT NULL,
    company_id integer NOT NULL,
    sdk_name character varying(255) NOT NULL,
    sdk_url character varying(255),
    is_open_source boolean DEFAULT false,
    has_third_party_tracking boolean DEFAULT true,
    license_type character varying(50) DEFAULT 'Commercial'::character varying,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    sdk_slug text
);


ALTER TABLE adtech.sdks OWNER TO postgres;

--
-- Name: version_code_sdk_scan_results; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.version_code_sdk_scan_results (
    id integer NOT NULL,
    version_code_id integer,
    scan_result smallint NOT NULL,
    scanned_at timestamp without time zone DEFAULT timezone('utc'::text, now()) NOT NULL
);


ALTER TABLE public.version_code_sdk_scan_results OWNER TO postgres;

--
-- Name: version_codes; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.version_codes (
    id integer NOT NULL,
    store_app integer NOT NULL,
    version_code character varying NOT NULL,
    updated_at timestamp without time zone DEFAULT timezone('utc'::text, now()),
    crawl_result smallint NOT NULL,
    apk_hash character varying(32),
    created_at timestamp without time zone DEFAULT timezone('utc'::text, now())
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
-- Name: store_app_sdk_strings; Type: MATERIALIZED VIEW; Schema: adtech; Owner: postgres
--

CREATE MATERIALIZED VIEW adtech.store_app_sdk_strings AS
 WITH latest_version_codes AS (
         SELECT DISTINCT ON (vc_1.store_app) vc_1.id,
            vc_1.store_app,
            vc_1.version_code,
            vc_1.updated_at,
            vc_1.crawl_result
           FROM (public.version_codes vc_1
             JOIN public.version_code_sdk_scan_results vcssr ON ((vc_1.id = vcssr.version_code_id)))
          WHERE (vcssr.scan_result = 1)
          ORDER BY vc_1.store_app, (string_to_array((vc_1.version_code)::text, '.'::text))::bigint[] DESC
        )
 SELECT vc.store_app,
    vdm.string_id AS version_string_id,
    css.sdk_id
   FROM ((latest_version_codes vc
     JOIN public.version_details_map vdm ON ((vc.id = vdm.version_code)))
     LEFT JOIN adtech.sdk_strings css ON ((vdm.string_id = css.version_string_id)))
  WITH NO DATA;


ALTER MATERIALIZED VIEW adtech.store_app_sdk_strings OWNER TO postgres;

--
-- Name: adstxt_crawl_results; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.adstxt_crawl_results (
    id integer NOT NULL,
    domain_id integer NOT NULL,
    crawl_result integer,
    crawled_at timestamp without time zone,
    created_at timestamp without time zone DEFAULT timezone('utc'::text, now()),
    updated_at timestamp without time zone DEFAULT timezone('utc'::text, now())
);


ALTER TABLE public.adstxt_crawl_results OWNER TO postgres;

--
-- Name: api_calls; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.api_calls (
    id integer NOT NULL,
    run_id integer NOT NULL,
    store_app integer NOT NULL,
    mitm_uuid uuid NOT NULL,
    flow_type text NOT NULL,
    tld_url text,
    status_code smallint NOT NULL,
    request_mime_type text,
    response_mime_type text,
    response_size_bytes integer,
    url text,
    ip_geo_snapshot_id integer,
    called_at timestamp without time zone NOT NULL,
    created_at timestamp with time zone DEFAULT timezone('utc'::text, now()) NOT NULL,
    updated_at timestamp with time zone DEFAULT timezone('utc'::text, now()) NOT NULL
);


ALTER TABLE public.api_calls OWNER TO postgres;

--
-- Name: app_global_metrics_history; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.app_global_metrics_history (
    snapshot_date date NOT NULL,
    store_app integer NOT NULL,
    installs bigint,
    rating_count bigint,
    review_count bigint,
    rating real,
    one_star bigint,
    two_star bigint,
    three_star bigint,
    four_star bigint,
    five_star bigint,
    store_last_updated date
);


ALTER TABLE public.app_global_metrics_history OWNER TO postgres;

--
-- Name: app_global_metrics_latest; Type: MATERIALIZED VIEW; Schema: public; Owner: postgres
--

CREATE MATERIALIZED VIEW public.app_global_metrics_latest AS
 SELECT DISTINCT ON (store_app) snapshot_date,
    store_app,
    installs,
    rating_count,
    review_count,
    rating,
    one_star,
    two_star,
    three_star,
    four_star,
    five_star,
    store_last_updated
   FROM public.app_global_metrics_history sacs
  ORDER BY store_app, snapshot_date DESC
  WITH NO DATA;


ALTER MATERIALIZED VIEW public.app_global_metrics_latest OWNER TO postgres;

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
    free boolean,
    price double precision,
    size text,
    minimum_android text,
    developer_email text,
    store_last_updated timestamp without time zone,
    content_rating text,
    ad_supported boolean,
    in_app_purchases boolean,
    created_at timestamp without time zone DEFAULT timezone('utc'::text, now()),
    updated_at timestamp without time zone DEFAULT timezone('utc'::text, now()),
    crawl_result integer,
    icon_url_512 character varying,
    release_date date,
    featured_image_url character varying,
    phone_image_url_1 character varying,
    phone_image_url_2 character varying,
    phone_image_url_3 character varying,
    tablet_image_url_1 character varying,
    tablet_image_url_2 character varying,
    tablet_image_url_3 character varying,
    textsearchable_index_col tsvector GENERATED ALWAYS AS (to_tsvector('simple'::regconfig, (COALESCE(name, ''::character varying))::text)) STORED,
    additional_html_scraped_at timestamp without time zone,
    icon_url_100 text
);


ALTER TABLE public.store_apps OWNER TO james;

--
-- Name: app_global_metrics_weekly_diffs; Type: MATERIALIZED VIEW; Schema: public; Owner: postgres
--

CREATE MATERIALIZED VIEW public.app_global_metrics_weekly_diffs AS
 WITH snapshot_diffs AS (
         SELECT sach.store_app,
            sach.snapshot_date,
            (date_trunc('week'::text, (sach.snapshot_date)::timestamp with time zone))::date AS week_start,
            (sach.installs - lag(sach.installs) OVER (PARTITION BY sach.store_app ORDER BY sach.snapshot_date)) AS installs_diff,
            (sach.rating_count - lag(sach.rating_count) OVER (PARTITION BY sach.store_app ORDER BY sach.snapshot_date)) AS rating_count_diff
           FROM public.app_global_metrics_history sach
          WHERE ((sach.store_app IN ( SELECT store_apps.id
                   FROM public.store_apps
                  WHERE (store_apps.crawl_result = 1))) AND (sach.snapshot_date > (CURRENT_DATE - '375 days'::interval)))
        )
 SELECT week_start,
    store_app,
    COALESCE(sum(installs_diff), (0)::numeric) AS installs_diff,
    COALESCE(sum(rating_count_diff), (0)::numeric) AS rating_count_diff
   FROM snapshot_diffs
  GROUP BY week_start, store_app
  ORDER BY week_start DESC, store_app
  WITH NO DATA;


ALTER MATERIALIZED VIEW public.app_global_metrics_weekly_diffs OWNER TO postgres;

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
 SELECT DISTINCT original_category,
        CASE
            WHEN (mapped_category = ANY (ARRAY['action'::text, 'casual'::text, 'adventure'::text, 'arcade'::text, 'board'::text, 'card'::text, 'casino'::text, 'puzzle'::text, 'racing'::text, 'simulation'::text, 'strategy'::text, 'trivia'::text, 'word'::text])) THEN ('game_'::text || mapped_category)
            WHEN (mapped_category = 'news_and_magazines'::text) THEN 'news'::text
            WHEN (mapped_category = 'educational'::text) THEN 'education'::text
            WHEN (mapped_category = 'book'::text) THEN 'books_and_reference'::text
            WHEN (mapped_category = 'navigation'::text) THEN 'maps_and_navigation'::text
            WHEN (mapped_category = 'music'::text) THEN 'music_and_audio'::text
            WHEN (mapped_category = 'photography'::text) THEN 'photo_and_video'::text
            WHEN (mapped_category = 'reference'::text) THEN 'books_and_reference'::text
            WHEN (mapped_category = 'role playing'::text) THEN 'game_role_playing'::text
            WHEN (mapped_category = 'social'::text) THEN 'social networking'::text
            WHEN (mapped_category = 'travel'::text) THEN 'travel_and_local'::text
            WHEN (mapped_category = 'utilities'::text) THEN 'tools'::text
            WHEN (mapped_category = 'video players_and_editors'::text) THEN 'video_players'::text
            WHEN (mapped_category = 'graphics_and_design'::text) THEN 'art_and_design'::text
            WHEN (mapped_category = 'parenting'::text) THEN 'family'::text
            WHEN (mapped_category IS NULL) THEN 'N/A'::text
            ELSE mapped_category
        END AS mapped_category
   FROM ( SELECT DISTINCT store_apps.category AS original_category,
            regexp_replace(lower((store_apps.category)::text), ' & '::text, '_and_'::text) AS mapped_category
           FROM public.store_apps) sub
  WITH NO DATA;


ALTER MATERIALIZED VIEW public.category_mapping OWNER TO postgres;

--
-- Name: creative_records; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.creative_records (
    id integer NOT NULL,
    api_call_id integer NOT NULL,
    creative_asset_id integer NOT NULL,
    creative_host_domain_id integer NOT NULL,
    creative_initial_domain_id integer,
    advertiser_store_app_id integer,
    advertiser_domain_id integer,
    mmp_domain_id integer,
    mmp_urls text[],
    additional_ad_domain_ids integer[],
    created_at timestamp with time zone DEFAULT timezone('utc'::text, now()) NOT NULL,
    updated_at timestamp with time zone DEFAULT timezone('utc'::text, now()) NOT NULL,
    click_ids integer[],
    click_url_ids integer[],
    CONSTRAINT check_advertiser_or_advertiser_domain CHECK (((advertiser_store_app_id IS NOT NULL) OR (advertiser_domain_id IS NOT NULL) OR ((advertiser_store_app_id IS NULL) AND (advertiser_domain_id IS NULL))))
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
    textsearchable_index_col tsvector GENERATED ALWAYS AS (to_tsvector('simple'::regconfig, (COALESCE(name, ''::character varying))::text)) STORED
);


ALTER TABLE public.developers OWNER TO james;

--
-- Name: domains; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.domains (
    id integer NOT NULL,
    domain_name character varying NOT NULL,
    created_at timestamp without time zone DEFAULT timezone('utc'::text, now())
);


ALTER TABLE public.domains OWNER TO postgres;

--
-- Name: store_app_z_scores; Type: MATERIALIZED VIEW; Schema: public; Owner: postgres
--

CREATE MATERIALIZED VIEW public.store_app_z_scores AS
 WITH latest_week AS (
         SELECT max(app_global_metrics_weekly_diffs.week_start) AS max_week
           FROM public.app_global_metrics_weekly_diffs
        ), latest_week_per_app AS (
         SELECT app_global_metrics_weekly_diffs.store_app,
            max(app_global_metrics_weekly_diffs.week_start) AS app_max_week
           FROM public.app_global_metrics_weekly_diffs
          GROUP BY app_global_metrics_weekly_diffs.store_app
        ), baseline_period AS (
         SELECT app_global_metrics_weekly_diffs.store_app,
            avg(app_global_metrics_weekly_diffs.installs_diff) AS avg_installs_diff,
            stddev(app_global_metrics_weekly_diffs.installs_diff) AS stddev_installs_diff,
            avg(app_global_metrics_weekly_diffs.rating_count_diff) AS avg_rating_diff,
            stddev(app_global_metrics_weekly_diffs.rating_count_diff) AS stddev_rating_diff
           FROM public.app_global_metrics_weekly_diffs,
            latest_week
          WHERE ((app_global_metrics_weekly_diffs.week_start >= (latest_week.max_week - '84 days'::interval)) AND (app_global_metrics_weekly_diffs.week_start <= (latest_week.max_week - '35 days'::interval)))
          GROUP BY app_global_metrics_weekly_diffs.store_app
        ), recent_data AS (
         SELECT s.store_app,
            lw.max_week,
            s.week_start,
            s.installs_diff,
            s.rating_count_diff,
                CASE
                    WHEN (s.week_start = lwpa.app_max_week) THEN 1
                    ELSE 0
                END AS is_latest_week,
                CASE
                    WHEN (s.week_start >= (lwpa.app_max_week - '14 days'::interval)) THEN 1
                    ELSE 0
                END AS in_2w_period,
                CASE
                    WHEN (s.week_start >= (lwpa.app_max_week - '28 days'::interval)) THEN 1
                    ELSE 0
                END AS in_4w_period
           FROM ((public.app_global_metrics_weekly_diffs s
             CROSS JOIN latest_week lw)
             JOIN latest_week_per_app lwpa ON ((s.store_app = lwpa.store_app)))
          WHERE (s.week_start >= (lw.max_week - '28 days'::interval))
        ), aggregated_metrics AS (
         SELECT rd.store_app,
            rd.max_week AS latest_week,
            sum(
                CASE
                    WHEN (rd.is_latest_week = 1) THEN rd.installs_diff
                    ELSE (0)::numeric
                END) AS installs_sum_1w,
            sum(
                CASE
                    WHEN (rd.is_latest_week = 1) THEN rd.rating_count_diff
                    ELSE ((0)::bigint)::numeric
                END) AS ratings_sum_1w,
            (sum(
                CASE
                    WHEN (rd.in_2w_period = 1) THEN rd.installs_diff
                    ELSE (0)::numeric
                END) / (NULLIF(sum(
                CASE
                    WHEN (rd.in_2w_period = 1) THEN 1
                    ELSE 0
                END), 0))::numeric) AS installs_avg_2w,
            (sum(
                CASE
                    WHEN (rd.in_2w_period = 1) THEN rd.rating_count_diff
                    ELSE ((0)::bigint)::numeric
                END) / (NULLIF(sum(
                CASE
                    WHEN (rd.in_2w_period = 1) THEN 1
                    ELSE 0
                END), 0))::numeric) AS ratings_avg_2w,
            sum(
                CASE
                    WHEN (rd.in_4w_period = 1) THEN rd.installs_diff
                    ELSE (0)::numeric
                END) AS installs_sum_4w,
            sum(
                CASE
                    WHEN (rd.in_4w_period = 1) THEN rd.rating_count_diff
                    ELSE ((0)::bigint)::numeric
                END) AS ratings_sum_4w,
            (sum(
                CASE
                    WHEN (rd.in_4w_period = 1) THEN rd.installs_diff
                    ELSE (0)::numeric
                END) / (NULLIF(sum(
                CASE
                    WHEN (rd.in_4w_period = 1) THEN 1
                    ELSE 0
                END), 0))::numeric) AS installs_avg_4w,
            (sum(
                CASE
                    WHEN (rd.in_4w_period = 1) THEN rd.rating_count_diff
                    ELSE ((0)::bigint)::numeric
                END) / (NULLIF(sum(
                CASE
                    WHEN (rd.in_4w_period = 1) THEN 1
                    ELSE 0
                END), 0))::numeric) AS ratings_avg_4w
           FROM recent_data rd
          GROUP BY rd.store_app, rd.max_week
        )
 SELECT am.store_app,
    am.latest_week,
    am.installs_sum_1w,
    am.ratings_sum_1w,
    am.installs_avg_2w,
    am.ratings_avg_2w,
    am.installs_sum_4w,
    am.ratings_sum_4w,
    am.installs_avg_4w,
    am.ratings_avg_4w,
    ((am.installs_avg_2w - bp.avg_installs_diff) / NULLIF(bp.stddev_installs_diff, (0)::numeric)) AS installs_z_score_2w,
    ((am.ratings_avg_2w - bp.avg_rating_diff) / NULLIF(bp.stddev_rating_diff, (0)::numeric)) AS ratings_z_score_2w,
    ((am.installs_avg_4w - bp.avg_installs_diff) / NULLIF(bp.stddev_installs_diff, (0)::numeric)) AS installs_z_score_4w,
    ((am.ratings_avg_4w - bp.avg_rating_diff) / NULLIF(bp.stddev_rating_diff, (0)::numeric)) AS ratings_z_score_4w
   FROM (aggregated_metrics am
     JOIN baseline_period bp ON ((am.store_app = bp.store_app)))
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
    run_at timestamp without time zone DEFAULT timezone('utc'::text, now()) NOT NULL
);


ALTER TABLE public.version_code_api_scan_results OWNER TO postgres;

--
-- Name: store_apps_overview; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.store_apps_overview AS
 WITH latest_version_codes AS (
         SELECT DISTINCT ON (version_codes.store_app) version_codes.id,
            version_codes.store_app,
            version_codes.version_code,
            version_codes.updated_at AS last_downloaded_at,
            version_codes.crawl_result AS download_result
           FROM public.version_codes
          WHERE ((version_codes.crawl_result = 1) AND (version_codes.updated_at >= '2025-05-01 00:00:00'::timestamp without time zone))
          ORDER BY version_codes.store_app, version_codes.updated_at DESC, (string_to_array((version_codes.version_code)::text, '.'::text))::bigint[] DESC
        ), latest_successful_version_codes AS (
         SELECT DISTINCT ON (vc.store_app) vc.id,
            vc.store_app,
            vc.version_code,
            vc.updated_at,
            vc.crawl_result
           FROM public.version_codes vc
          WHERE (vc.crawl_result = 1)
          ORDER BY vc.store_app, (string_to_array((vc.version_code)::text, '.'::text))::bigint[] DESC
        ), last_sdk_scan AS (
         SELECT DISTINCT ON (vc.store_app) vc.store_app,
            lsscr.version_code_id AS version_code,
            lsscr.scanned_at,
            lsscr.scan_result
           FROM (public.version_code_sdk_scan_results lsscr
             LEFT JOIN public.version_codes vc ON ((lsscr.version_code_id = vc.id)))
          ORDER BY vc.store_app, lsscr.scanned_at DESC
        ), last_successful_sdk_scan AS (
         SELECT DISTINCT ON (vc.store_app) vc.id,
            vc.store_app,
            vc.version_code,
            vcss.scanned_at,
            vcss.scan_result
           FROM (public.version_codes vc
             LEFT JOIN public.version_code_sdk_scan_results vcss ON ((vc.id = vcss.version_code_id)))
          WHERE (vcss.scan_result = 1)
          ORDER BY vc.store_app, vcss.scanned_at DESC, (string_to_array((vc.version_code)::text, '.'::text))::bigint[] DESC
        ), latest_en_descriptions AS (
         SELECT DISTINCT ON (store_apps_descriptions.store_app) store_apps_descriptions.store_app,
            store_apps_descriptions.description,
            store_apps_descriptions.description_short
           FROM public.store_apps_descriptions
          WHERE (store_apps_descriptions.language_id = 1)
          ORDER BY store_apps_descriptions.store_app, store_apps_descriptions.updated_at DESC
        ), latest_api_calls AS (
         SELECT DISTINCT ON (vc.store_app) vc.store_app,
            vasr.run_result,
            vasr.run_at
           FROM (public.version_codes vc
             LEFT JOIN public.version_code_api_scan_results vasr ON ((vc.id = vasr.version_code_id)))
          WHERE (vc.updated_at >= '2025-05-01 00:00:00'::timestamp without time zone)
        ), latest_successful_api_calls AS (
         SELECT DISTINCT ON (vc.store_app) vc.store_app,
            vasr.run_at
           FROM (public.version_codes vc
             LEFT JOIN public.version_code_api_scan_results vasr ON ((vc.id = vasr.version_code_id)))
          WHERE ((vasr.run_result = 1) AND (vc.updated_at >= '2025-05-01 00:00:00'::timestamp without time zone))
        ), my_ad_creatives AS (
         SELECT cr.advertiser_store_app_id AS store_app,
            count(*) AS ad_creative_count
           FROM public.creative_records cr
          GROUP BY cr.advertiser_store_app_id
        ), my_mon_creatives AS (
         SELECT DISTINCT 1 AS ad_mon_creatives,
            ac.store_app
           FROM (public.creative_records cr
             LEFT JOIN public.api_calls ac ON ((cr.api_call_id = ac.id)))
        ), app_metrics AS (
         SELECT app_global_metrics_latest.store_app,
            app_global_metrics_latest.rating,
            app_global_metrics_latest.rating_count,
            app_global_metrics_latest.installs
           FROM public.app_global_metrics_latest
        )
 SELECT sa.id,
    sa.name,
    sa.store_id,
    sa.store,
    cm.mapped_category AS category,
    am.rating,
    am.rating_count,
    am.installs,
    saz.installs_sum_1w,
    saz.ratings_sum_1w,
    saz.installs_sum_4w,
    saz.ratings_sum_4w,
    saz.installs_z_score_2w,
    saz.ratings_z_score_2w,
    saz.installs_z_score_4w,
    saz.ratings_z_score_4w,
    sa.ad_supported,
    sa.free,
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
    to_tsvector('simple'::regconfig, (((((COALESCE(sa.name, ''::character varying))::text || ' '::text) || (COALESCE(sa.store_id, ''::character varying))::text) || ' '::text) || (COALESCE(d.name, ''::character varying))::text)) AS textsearchable,
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
   FROM ((((((((((((((((public.store_apps sa
     LEFT JOIN public.category_mapping cm ON (((sa.category)::text = (cm.original_category)::text)))
     LEFT JOIN public.developers d ON ((sa.developer = d.id)))
     LEFT JOIN public.app_urls_map aum ON ((sa.id = aum.store_app)))
     LEFT JOIN public.domains pd ON ((aum.pub_domain = pd.id)))
     LEFT JOIN public.adstxt_crawl_results pdcr ON ((pd.id = pdcr.domain_id)))
     LEFT JOIN latest_version_codes lvc ON ((sa.id = lvc.store_app)))
     LEFT JOIN last_sdk_scan lss ON ((sa.id = lss.store_app)))
     LEFT JOIN last_successful_sdk_scan lsss ON ((sa.id = lsss.store_app)))
     LEFT JOIN latest_successful_version_codes lsvc ON ((sa.id = lsvc.store_app)))
     LEFT JOIN latest_en_descriptions ld ON ((sa.id = ld.store_app)))
     LEFT JOIN public.store_app_z_scores saz ON ((sa.id = saz.store_app)))
     LEFT JOIN latest_api_calls lac ON ((sa.id = lac.store_app)))
     LEFT JOIN latest_successful_api_calls lsac ON ((sa.id = lsac.store_app)))
     LEFT JOIN my_ad_creatives acr ON ((sa.id = acr.store_app)))
     LEFT JOIN my_mon_creatives amc ON ((sa.id = amc.store_app)))
     LEFT JOIN app_metrics am ON ((sa.id = am.store_app)))
  WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.store_apps_overview OWNER TO postgres;

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
-- Name: combined_store_apps_companies; Type: MATERIALIZED VIEW; Schema: adtech; Owner: postgres
--

CREATE MATERIALIZED VIEW adtech.combined_store_apps_companies AS
 WITH api_based_companies AS (
         SELECT DISTINCT saac.store_app,
            cm.mapped_category AS app_category,
            cdm.company_id,
            c_1.parent_company_id AS parent_id,
            'api_call'::text AS tag_source,
            COALESCE(cad_1.domain_name, (saac.tld_url)::character varying) AS ad_domain
           FROM ((((((public.api_calls saac
             LEFT JOIN public.store_apps sa_1 ON ((saac.store_app = sa_1.id)))
             LEFT JOIN public.category_mapping cm ON (((sa_1.category)::text = (cm.original_category)::text)))
             LEFT JOIN public.domains ad_1 ON ((saac.tld_url = (ad_1.domain_name)::text)))
             LEFT JOIN adtech.company_domain_mapping cdm ON ((ad_1.id = cdm.domain_id)))
             LEFT JOIN adtech.companies c_1 ON ((cdm.company_id = c_1.id)))
             LEFT JOIN public.domains cad_1 ON ((c_1.domain_id = cad_1.id)))
        ), developer_based_companies AS (
         SELECT DISTINCT sa_1.id AS store_app,
            cm.mapped_category AS app_category,
            cd.company_id,
            d.domain_name AS ad_domain,
            'developer'::text AS tag_source,
            COALESCE(c_1.parent_company_id, cd.company_id) AS parent_id
           FROM ((((adtech.company_developers cd
             LEFT JOIN public.store_apps sa_1 ON ((cd.developer_id = sa_1.developer)))
             LEFT JOIN adtech.companies c_1 ON ((cd.company_id = c_1.id)))
             LEFT JOIN public.domains d ON ((c_1.domain_id = d.id)))
             LEFT JOIN public.category_mapping cm ON (((sa_1.category)::text = (cm.original_category)::text)))
        ), sdk_based_companies AS (
         SELECT DISTINCT sasd.store_app,
            cm.mapped_category AS app_category,
            sac.company_id,
            ad_1.domain_name AS ad_domain,
            'sdk'::text AS tag_source,
            COALESCE(c_1.parent_company_id, sac.company_id) AS parent_id
           FROM (((((adtech.store_app_sdk_strings sasd
             LEFT JOIN adtech.sdks sac ON ((sac.id = sasd.sdk_id)))
             LEFT JOIN adtech.companies c_1 ON ((sac.company_id = c_1.id)))
             LEFT JOIN public.domains ad_1 ON ((c_1.domain_id = ad_1.id)))
             LEFT JOIN public.store_apps sa_1 ON ((sasd.store_app = sa_1.id)))
             LEFT JOIN public.category_mapping cm ON (((sa_1.category)::text = (cm.original_category)::text)))
        ), distinct_ad_and_pub_domains AS (
         SELECT DISTINCT pd.domain_name AS publisher_domain_url,
            ad_1.domain_name AS ad_domain_url,
            aae.relationship
           FROM ((((public.app_ads_entrys aae
             LEFT JOIN public.domains ad_1 ON ((aae.ad_domain = ad_1.id)))
             LEFT JOIN public.app_ads_map aam ON ((aae.id = aam.app_ads_entry)))
             LEFT JOIN public.domains pd ON ((aam.pub_domain = pd.id)))
             LEFT JOIN public.adstxt_crawl_results pdcr ON ((pd.id = pdcr.domain_id)))
          WHERE ((pdcr.crawled_at - aam.updated_at) < '01:00:00'::interval)
        ), adstxt_based_companies AS (
         SELECT DISTINCT aum.store_app,
            cm.mapped_category AS app_category,
            c_1.id AS company_id,
            pnv.ad_domain_url AS ad_domain,
            COALESCE(c_1.parent_company_id, c_1.id) AS parent_id,
                CASE
                    WHEN ((pnv.relationship)::text = 'DIRECT'::text) THEN 'app_ads_direct'::text
                    WHEN ((pnv.relationship)::text = 'RESELLER'::text) THEN 'app_ads_reseller'::text
                    ELSE 'app_ads_unknown'::text
                END AS tag_source
           FROM ((((((public.app_urls_map aum
             LEFT JOIN public.domains pd ON ((aum.pub_domain = pd.id)))
             LEFT JOIN distinct_ad_and_pub_domains pnv ON (((pd.domain_name)::text = (pnv.publisher_domain_url)::text)))
             LEFT JOIN public.domains ad_1 ON (((pnv.ad_domain_url)::text = (ad_1.domain_name)::text)))
             LEFT JOIN adtech.companies c_1 ON ((ad_1.id = c_1.domain_id)))
             LEFT JOIN public.store_apps sa_1 ON ((aum.store_app = sa_1.id)))
             LEFT JOIN public.category_mapping cm ON (((sa_1.category)::text = (cm.original_category)::text)))
          WHERE ((sa_1.crawl_result = 1) AND ((pnv.ad_domain_url IS NOT NULL) OR (c_1.id IS NOT NULL)))
        ), combined_sources AS (
         SELECT api_based_companies.store_app,
            api_based_companies.app_category,
            api_based_companies.company_id,
            api_based_companies.parent_id,
            api_based_companies.ad_domain,
            api_based_companies.tag_source
           FROM api_based_companies
        UNION ALL
         SELECT sdk_based_companies.store_app,
            sdk_based_companies.app_category,
            sdk_based_companies.company_id,
            sdk_based_companies.parent_id,
            sdk_based_companies.ad_domain,
            sdk_based_companies.tag_source
           FROM sdk_based_companies
        UNION ALL
         SELECT adstxt_based_companies.store_app,
            adstxt_based_companies.app_category,
            adstxt_based_companies.company_id,
            adstxt_based_companies.parent_id,
            adstxt_based_companies.ad_domain,
            adstxt_based_companies.tag_source
           FROM adstxt_based_companies
        UNION ALL
         SELECT developer_based_companies.store_app,
            developer_based_companies.app_category,
            developer_based_companies.company_id,
            developer_based_companies.parent_id,
            developer_based_companies.ad_domain,
            developer_based_companies.tag_source
           FROM developer_based_companies
        )
 SELECT cs.ad_domain,
    cs.store_app,
    sa.category AS app_category,
    c.id AS company_id,
    COALESCE(c.parent_company_id, c.id) AS parent_id,
        CASE
            WHEN (sa.sdk_successful_last_crawled IS NOT NULL) THEN bool_or((cs.tag_source = 'sdk'::text))
            ELSE NULL::boolean
        END AS sdk,
        CASE
            WHEN (sa.api_successful_last_crawled IS NOT NULL) THEN bool_or((cs.tag_source = 'api_call'::text))
            ELSE NULL::boolean
        END AS api_call,
    bool_or((cs.tag_source = 'app_ads_direct'::text)) AS app_ads_direct,
    bool_or((cs.tag_source = 'app_ads_reseller'::text)) AS app_ads_reseller
   FROM (((combined_sources cs
     LEFT JOIN frontend.store_apps_overview sa ON ((cs.store_app = sa.id)))
     LEFT JOIN public.domains ad ON (((cs.ad_domain)::text = (ad.domain_name)::text)))
     LEFT JOIN adtech.companies c ON ((ad.id = c.domain_id)))
  GROUP BY cs.ad_domain, cs.store_app, sa.category, c.id, c.parent_company_id, sa.sdk_successful_last_crawled, sa.api_successful_last_crawled
  WITH NO DATA;


ALTER MATERIALIZED VIEW adtech.combined_store_apps_companies OWNER TO postgres;

--
-- Name: store_app_sdk_strings_2025_h1; Type: MATERIALIZED VIEW; Schema: adtech; Owner: postgres
--

CREATE MATERIALIZED VIEW adtech.store_app_sdk_strings_2025_h1 AS
 WITH latest_version_codes AS (
         SELECT DISTINCT ON (vc_1.store_app) vc_1.id,
            vc_1.store_app,
            vc_1.version_code,
            vc_1.updated_at,
            vc_1.crawl_result
           FROM (public.version_codes vc_1
             JOIN public.version_code_sdk_scan_results vcssr ON ((vc_1.id = vcssr.version_code_id)))
          WHERE ((vcssr.scan_result = 1) AND (vc_1.updated_at >= '2025-01-01 00:00:00'::timestamp without time zone) AND (vc_1.updated_at < '2025-07-01 00:00:00'::timestamp without time zone))
          ORDER BY vc_1.store_app, (string_to_array((vc_1.version_code)::text, '.'::text))::bigint[] DESC
        )
 SELECT vc.store_app,
    vdm.string_id AS version_string_id,
    sd.id AS sdk_id
   FROM (((latest_version_codes vc
     JOIN public.version_details_map vdm ON ((vc.id = vdm.version_code)))
     JOIN adtech.sdk_strings css ON ((vdm.string_id = css.version_string_id)))
     JOIN adtech.sdks sd ON ((css.sdk_id = sd.id)))
  WITH NO DATA;


ALTER MATERIALIZED VIEW adtech.store_app_sdk_strings_2025_h1 OWNER TO postgres;

--
-- Name: combined_store_apps_companies_2025_h1; Type: MATERIALIZED VIEW; Schema: adtech; Owner: postgres
--

CREATE MATERIALIZED VIEW adtech.combined_store_apps_companies_2025_h1 AS
 WITH api_based_companies AS (
         SELECT DISTINCT saac.store_app,
            cm.mapped_category AS app_category,
            cdm.company_id,
            c_1.parent_company_id AS parent_id,
            'api_call'::text AS tag_source,
            COALESCE(cad_1.domain_name, (saac.tld_url)::character varying) AS ad_domain
           FROM ((((((public.api_calls saac
             LEFT JOIN public.store_apps sa_1 ON ((saac.store_app = sa_1.id)))
             LEFT JOIN public.category_mapping cm ON (((sa_1.category)::text = (cm.original_category)::text)))
             LEFT JOIN public.domains ad_1 ON ((saac.tld_url = (ad_1.domain_name)::text)))
             LEFT JOIN adtech.company_domain_mapping cdm ON ((ad_1.id = cdm.domain_id)))
             LEFT JOIN adtech.companies c_1 ON ((cdm.company_id = c_1.id)))
             LEFT JOIN public.domains cad_1 ON ((c_1.domain_id = cad_1.id)))
          WHERE ((saac.called_at >= '2025-01-01 00:00:00'::timestamp without time zone) AND (saac.called_at < '2025-07-01 00:00:00'::timestamp without time zone))
        ), developer_based_companies AS (
         SELECT DISTINCT sa_1.id AS store_app,
            cm.mapped_category AS app_category,
            cd.company_id,
            d.domain_name AS ad_domain,
            'developer'::text AS tag_source,
            COALESCE(c_1.parent_company_id, cd.company_id) AS parent_id
           FROM ((((adtech.company_developers cd
             LEFT JOIN public.store_apps sa_1 ON ((cd.developer_id = sa_1.developer)))
             LEFT JOIN adtech.companies c_1 ON ((cd.company_id = c_1.id)))
             LEFT JOIN public.domains d ON ((c_1.domain_id = d.id)))
             LEFT JOIN public.category_mapping cm ON (((sa_1.category)::text = (cm.original_category)::text)))
        ), sdk_based_companies AS (
         SELECT DISTINCT sasd.store_app,
            cm.mapped_category AS app_category,
            sac.company_id,
            ad_1.domain_name AS ad_domain,
            'sdk'::text AS tag_source,
            COALESCE(c_1.parent_company_id, sac.company_id) AS parent_id
           FROM (((((adtech.store_app_sdk_strings_2025_h1 sasd
             LEFT JOIN adtech.sdks sac ON ((sac.id = sasd.sdk_id)))
             LEFT JOIN adtech.companies c_1 ON ((sac.company_id = c_1.id)))
             LEFT JOIN public.domains ad_1 ON ((c_1.domain_id = ad_1.id)))
             LEFT JOIN public.store_apps sa_1 ON ((sasd.store_app = sa_1.id)))
             LEFT JOIN public.category_mapping cm ON (((sa_1.category)::text = (cm.original_category)::text)))
        ), distinct_ad_and_pub_domains AS (
         SELECT DISTINCT pd.domain_name AS publisher_domain_url,
            ad_1.domain_name AS ad_domain_url,
            aae.relationship
           FROM ((((public.app_ads_entrys aae
             LEFT JOIN public.domains ad_1 ON ((aae.ad_domain = ad_1.id)))
             LEFT JOIN public.app_ads_map aam ON ((aae.id = aam.app_ads_entry)))
             LEFT JOIN public.domains pd ON ((aam.pub_domain = pd.id)))
             LEFT JOIN public.adstxt_crawl_results pdcr ON ((pd.id = pdcr.domain_id)))
          WHERE ((pdcr.crawled_at - aam.updated_at) < '01:00:00'::interval)
        ), combined_sources AS (
         SELECT api_based_companies.store_app,
            api_based_companies.app_category,
            api_based_companies.company_id,
            api_based_companies.parent_id,
            api_based_companies.ad_domain,
            api_based_companies.tag_source
           FROM api_based_companies
        UNION ALL
         SELECT sdk_based_companies.store_app,
            sdk_based_companies.app_category,
            sdk_based_companies.company_id,
            sdk_based_companies.parent_id,
            sdk_based_companies.ad_domain,
            sdk_based_companies.tag_source
           FROM sdk_based_companies
        )
 SELECT cs.ad_domain,
    cs.store_app,
    sa.category AS app_category,
    c.id AS company_id,
    COALESCE(c.parent_company_id, c.id) AS parent_id,
        CASE
            WHEN (sa.sdk_successful_last_crawled IS NOT NULL) THEN bool_or((cs.tag_source = 'sdk'::text))
            ELSE NULL::boolean
        END AS sdk,
        CASE
            WHEN (sa.api_successful_last_crawled IS NOT NULL) THEN bool_or((cs.tag_source = 'api_call'::text))
            ELSE NULL::boolean
        END AS api_call,
    bool_or((cs.tag_source = 'app_ads_direct'::text)) AS app_ads_direct,
    bool_or((cs.tag_source = 'app_ads_reseller'::text)) AS app_ads_reseller
   FROM (((combined_sources cs
     LEFT JOIN frontend.store_apps_overview sa ON ((cs.store_app = sa.id)))
     LEFT JOIN public.domains ad ON (((cs.ad_domain)::text = (ad.domain_name)::text)))
     LEFT JOIN adtech.companies c ON ((ad.id = c.domain_id)))
  GROUP BY cs.ad_domain, cs.store_app, sa.category, c.id, c.parent_company_id, sa.sdk_successful_last_crawled, sa.api_successful_last_crawled
  WITH NO DATA;


ALTER MATERIALIZED VIEW adtech.combined_store_apps_companies_2025_h1 OWNER TO postgres;

--
-- Name: store_app_sdk_strings_2025_h2; Type: MATERIALIZED VIEW; Schema: adtech; Owner: postgres
--

CREATE MATERIALIZED VIEW adtech.store_app_sdk_strings_2025_h2 AS
 WITH latest_version_codes AS (
         SELECT DISTINCT ON (vc_1.store_app) vc_1.id,
            vc_1.store_app,
            vc_1.version_code,
            vc_1.updated_at,
            vc_1.crawl_result
           FROM (public.version_codes vc_1
             JOIN public.version_code_sdk_scan_results vcssr ON ((vc_1.id = vcssr.version_code_id)))
          WHERE ((vcssr.scan_result = 1) AND (vc_1.updated_at >= '2025-01-01 00:00:00'::timestamp without time zone) AND (vc_1.updated_at < '2026-01-01 00:00:00'::timestamp without time zone))
          ORDER BY vc_1.store_app, (string_to_array((vc_1.version_code)::text, '.'::text))::bigint[] DESC
        )
 SELECT vc.store_app,
    vdm.string_id AS version_string_id,
    sd.id AS sdk_id
   FROM (((latest_version_codes vc
     JOIN public.version_details_map vdm ON ((vc.id = vdm.version_code)))
     JOIN adtech.sdk_strings css ON ((vdm.string_id = css.version_string_id)))
     JOIN adtech.sdks sd ON ((css.sdk_id = sd.id)))
  WITH NO DATA;


ALTER MATERIALIZED VIEW adtech.store_app_sdk_strings_2025_h2 OWNER TO postgres;

--
-- Name: combined_store_apps_companies_2025_h2; Type: MATERIALIZED VIEW; Schema: adtech; Owner: postgres
--

CREATE MATERIALIZED VIEW adtech.combined_store_apps_companies_2025_h2 AS
 WITH api_based_companies AS (
         SELECT DISTINCT saac.store_app,
            cm.mapped_category AS app_category,
            cdm.company_id,
            c_1.parent_company_id AS parent_id,
            'api_call'::text AS tag_source,
            COALESCE(cad_1.domain_name, (saac.tld_url)::character varying) AS ad_domain
           FROM ((((((public.api_calls saac
             LEFT JOIN public.store_apps sa_1 ON ((saac.store_app = sa_1.id)))
             LEFT JOIN public.category_mapping cm ON (((sa_1.category)::text = (cm.original_category)::text)))
             LEFT JOIN public.domains ad_1 ON ((saac.tld_url = (ad_1.domain_name)::text)))
             LEFT JOIN adtech.company_domain_mapping cdm ON ((ad_1.id = cdm.domain_id)))
             LEFT JOIN adtech.companies c_1 ON ((cdm.company_id = c_1.id)))
             LEFT JOIN public.domains cad_1 ON ((c_1.domain_id = cad_1.id)))
          WHERE ((saac.called_at >= '2025-01-01 00:00:00'::timestamp without time zone) AND (saac.called_at < '2026-01-01 00:00:00'::timestamp without time zone))
        ), developer_based_companies AS (
         SELECT DISTINCT sa_1.id AS store_app,
            cm.mapped_category AS app_category,
            cd.company_id,
            d.domain_name AS ad_domain,
            'developer'::text AS tag_source,
            COALESCE(c_1.parent_company_id, cd.company_id) AS parent_id
           FROM ((((adtech.company_developers cd
             LEFT JOIN public.store_apps sa_1 ON ((cd.developer_id = sa_1.developer)))
             LEFT JOIN adtech.companies c_1 ON ((cd.company_id = c_1.id)))
             LEFT JOIN public.domains d ON ((c_1.domain_id = d.id)))
             LEFT JOIN public.category_mapping cm ON (((sa_1.category)::text = (cm.original_category)::text)))
        ), sdk_based_companies AS (
         SELECT DISTINCT sasd.store_app,
            cm.mapped_category AS app_category,
            sac.company_id,
            ad_1.domain_name AS ad_domain,
            'sdk'::text AS tag_source,
            COALESCE(c_1.parent_company_id, sac.company_id) AS parent_id
           FROM (((((adtech.store_app_sdk_strings_2025_h2 sasd
             LEFT JOIN adtech.sdks sac ON ((sac.id = sasd.sdk_id)))
             LEFT JOIN adtech.companies c_1 ON ((sac.company_id = c_1.id)))
             LEFT JOIN public.domains ad_1 ON ((c_1.domain_id = ad_1.id)))
             LEFT JOIN public.store_apps sa_1 ON ((sasd.store_app = sa_1.id)))
             LEFT JOIN public.category_mapping cm ON (((sa_1.category)::text = (cm.original_category)::text)))
        ), distinct_ad_and_pub_domains AS (
         SELECT DISTINCT pd.domain_name AS publisher_domain_url,
            ad_1.domain_name AS ad_domain_url,
            aae.relationship
           FROM ((((public.app_ads_entrys aae
             LEFT JOIN public.domains ad_1 ON ((aae.ad_domain = ad_1.id)))
             LEFT JOIN public.app_ads_map aam ON ((aae.id = aam.app_ads_entry)))
             LEFT JOIN public.domains pd ON ((aam.pub_domain = pd.id)))
             LEFT JOIN public.adstxt_crawl_results pdcr ON ((pd.id = pdcr.domain_id)))
          WHERE ((pdcr.crawled_at - aam.updated_at) < '01:00:00'::interval)
        ), combined_sources AS (
         SELECT api_based_companies.store_app,
            api_based_companies.app_category,
            api_based_companies.company_id,
            api_based_companies.parent_id,
            api_based_companies.ad_domain,
            api_based_companies.tag_source
           FROM api_based_companies
        UNION ALL
         SELECT sdk_based_companies.store_app,
            sdk_based_companies.app_category,
            sdk_based_companies.company_id,
            sdk_based_companies.parent_id,
            sdk_based_companies.ad_domain,
            sdk_based_companies.tag_source
           FROM sdk_based_companies
        )
 SELECT cs.ad_domain,
    cs.store_app,
    sa.category AS app_category,
    c.id AS company_id,
    COALESCE(c.parent_company_id, c.id) AS parent_id,
        CASE
            WHEN (sa.sdk_successful_last_crawled IS NOT NULL) THEN bool_or((cs.tag_source = 'sdk'::text))
            ELSE NULL::boolean
        END AS sdk,
        CASE
            WHEN (sa.api_successful_last_crawled IS NOT NULL) THEN bool_or((cs.tag_source = 'api_call'::text))
            ELSE NULL::boolean
        END AS api_call,
    bool_or((cs.tag_source = 'app_ads_direct'::text)) AS app_ads_direct,
    bool_or((cs.tag_source = 'app_ads_reseller'::text)) AS app_ads_reseller
   FROM (((combined_sources cs
     LEFT JOIN frontend.store_apps_overview sa ON ((cs.store_app = sa.id)))
     LEFT JOIN public.domains ad ON (((cs.ad_domain)::text = (ad.domain_name)::text)))
     LEFT JOIN adtech.companies c ON ((ad.id = c.domain_id)))
  GROUP BY cs.ad_domain, cs.store_app, sa.category, c.id, c.parent_company_id, sa.sdk_successful_last_crawled, sa.api_successful_last_crawled
  WITH NO DATA;


ALTER MATERIALIZED VIEW adtech.combined_store_apps_companies_2025_h2 OWNER TO postgres;

--
-- Name: combined_store_apps_parent_companies; Type: MATERIALIZED VIEW; Schema: adtech; Owner: postgres
--

CREATE MATERIALIZED VIEW adtech.combined_store_apps_parent_companies AS
 SELECT csac.store_app,
    csac.app_category,
    csac.parent_id AS company_id,
    COALESCE(ad.domain_name, csac.ad_domain) AS ad_domain,
    bool_or(csac.sdk) AS sdk,
    bool_or(csac.api_call) AS api_call,
    bool_or(csac.app_ads_direct) AS app_ads_direct
   FROM ((adtech.combined_store_apps_companies csac
     LEFT JOIN adtech.companies c ON ((csac.parent_id = c.id)))
     LEFT JOIN public.domains ad ON ((c.domain_id = ad.id)))
  WHERE (csac.parent_id IN ( SELECT DISTINCT pc.id
           FROM (adtech.companies pc
             LEFT JOIN adtech.companies c_1 ON ((pc.id = c_1.parent_company_id)))
          WHERE (c_1.id IS NOT NULL)))
  GROUP BY COALESCE(ad.domain_name, csac.ad_domain), csac.store_app, csac.app_category, csac.parent_id
  WITH NO DATA;


ALTER MATERIALIZED VIEW adtech.combined_store_apps_parent_companies OWNER TO postgres;

--
-- Name: companies_id_seq; Type: SEQUENCE; Schema: adtech; Owner: postgres
--

ALTER TABLE adtech.companies ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME adtech.companies_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: sdk_categories; Type: TABLE; Schema: adtech; Owner: postgres
--

CREATE TABLE adtech.sdk_categories (
    sdk_id integer NOT NULL,
    category_id integer NOT NULL
);


ALTER TABLE adtech.sdk_categories OWNER TO postgres;

--
-- Name: company_categories; Type: MATERIALIZED VIEW; Schema: adtech; Owner: postgres
--

CREATE MATERIALIZED VIEW adtech.company_categories AS
 SELECT DISTINCT c.id AS company_id,
    sc.category_id
   FROM ((adtech.sdk_categories sc
     LEFT JOIN adtech.sdks sd ON ((sc.sdk_id = sd.id)))
     JOIN adtech.companies c ON ((sd.company_id = c.id)))
  WITH NO DATA;


ALTER MATERIALIZED VIEW adtech.company_categories OWNER TO postgres;

--
-- Name: company_mediation_adapters; Type: TABLE; Schema: adtech; Owner: postgres
--

CREATE TABLE adtech.company_mediation_adapters (
    company_id integer NOT NULL,
    adapter_pattern character varying(100) NOT NULL
);


ALTER TABLE adtech.company_mediation_adapters OWNER TO postgres;

--
-- Name: store_app_ranks_weekly; Type: TABLE; Schema: frontend; Owner: postgres
--

CREATE TABLE frontend.store_app_ranks_weekly (
    rank smallint NOT NULL,
    best_rank smallint NOT NULL,
    country smallint NOT NULL,
    store_collection smallint NOT NULL,
    store_category smallint NOT NULL,
    crawled_date date NOT NULL,
    store_app integer NOT NULL
);


ALTER TABLE frontend.store_app_ranks_weekly OWNER TO postgres;

--
-- Name: company_share_change_2025; Type: MATERIALIZED VIEW; Schema: adtech; Owner: postgres
--

CREATE MATERIALIZED VIEW adtech.company_share_change_2025 AS
 WITH limit_apps AS (
         SELECT DISTINCT store_app_ranks_weekly.store_app
           FROM frontend.store_app_ranks_weekly
          WHERE ((store_app_ranks_weekly.crawled_date >= '2025-01-01'::date) AND (store_app_ranks_weekly.crawled_date < '2026-01-01'::date))
        ), totals AS (
         SELECT 'h1'::text AS half,
            count(DISTINCT combined_store_apps_companies_2025_h1.store_app) AS total_apps
           FROM adtech.combined_store_apps_companies_2025_h1
          WHERE (combined_store_apps_companies_2025_h1.sdk AND (combined_store_apps_companies_2025_h1.store_app IN ( SELECT limit_apps.store_app
                   FROM limit_apps)))
        UNION ALL
         SELECT 'h2'::text AS half,
            count(DISTINCT combined_store_apps_companies_2025_h2.store_app) AS total_apps
           FROM adtech.combined_store_apps_companies_2025_h2
          WHERE (combined_store_apps_companies_2025_h2.sdk AND (combined_store_apps_companies_2025_h2.store_app IN ( SELECT limit_apps.store_app
                   FROM limit_apps)))
        ), domain_counts AS (
         SELECT 'h1'::text AS half,
            combined_store_apps_companies_2025_h1.ad_domain,
            count(DISTINCT combined_store_apps_companies_2025_h1.store_app) AS app_count
           FROM adtech.combined_store_apps_companies_2025_h1
          WHERE (combined_store_apps_companies_2025_h1.sdk AND (combined_store_apps_companies_2025_h1.store_app IN ( SELECT limit_apps.store_app
                   FROM limit_apps)))
          GROUP BY combined_store_apps_companies_2025_h1.ad_domain
        UNION ALL
         SELECT 'h2'::text AS half,
            combined_store_apps_companies_2025_h2.ad_domain,
            count(DISTINCT combined_store_apps_companies_2025_h2.store_app) AS app_count
           FROM adtech.combined_store_apps_companies_2025_h2
          WHERE (combined_store_apps_companies_2025_h2.sdk AND (combined_store_apps_companies_2025_h2.store_app IN ( SELECT limit_apps.store_app
                   FROM limit_apps)))
          GROUP BY combined_store_apps_companies_2025_h2.ad_domain
        ), shares AS (
         SELECT d.half,
            d.ad_domain,
            d.app_count,
            t.total_apps,
            ((d.app_count)::numeric / (NULLIF(t.total_apps, 0))::numeric) AS pct_share
           FROM (domain_counts d
             JOIN totals t ON ((t.half = d.half)))
        ), shares_h1 AS (
         SELECT shares.half,
            shares.ad_domain,
            shares.app_count,
            shares.total_apps,
            shares.pct_share
           FROM shares
          WHERE (shares.half = 'h1'::text)
        ), shares_h2 AS (
         SELECT shares.half,
            shares.ad_domain,
            shares.app_count,
            shares.total_apps,
            shares.pct_share
           FROM shares
          WHERE (shares.half = 'h2'::text)
        )
 SELECT COALESCE(s2.ad_domain, s1.ad_domain) AS ad_domain,
    s1.app_count AS apps_h1,
    s1.total_apps AS total_apps_h1,
    round((COALESCE(s1.pct_share, (0)::numeric) * (100)::numeric), 4) AS share_h1_pct,
    s2.app_count AS apps_h2,
    s2.total_apps AS total_apps_h2,
    round((COALESCE(s2.pct_share, (0)::numeric) * (100)::numeric), 4) AS share_h2_pct,
    (COALESCE(s2.app_count, (0)::bigint) - COALESCE(s1.app_count, (0)::bigint)) AS net_app_change,
        CASE
            WHEN ((s1.app_count IS NULL) OR (s1.app_count = 0)) THEN 100.00
            ELSE round(((((COALESCE(s2.app_count, (0)::bigint) - s1.app_count))::numeric / (s1.app_count)::numeric) * (100)::numeric), 2)
        END AS app_growth_pct,
    round(((COALESCE(s2.pct_share, (0)::numeric) - COALESCE(s1.pct_share, (0)::numeric)) * (100)::numeric), 6) AS share_change_pp
   FROM (shares_h1 s1
     FULL JOIN shares_h2 s2 ON (((s1.ad_domain)::text = (s2.ad_domain)::text)))
  ORDER BY (round(((COALESCE(s2.pct_share, (0)::numeric) - COALESCE(s1.pct_share, (0)::numeric)) * (100)::numeric), 6)) DESC NULLS LAST
  WITH NO DATA;


ALTER MATERIALIZED VIEW adtech.company_share_change_2025 OWNER TO postgres;

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
-- Name: sdk_packages_id_seq; Type: SEQUENCE; Schema: adtech; Owner: postgres
--

ALTER TABLE adtech.sdk_packages ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME adtech.sdk_packages_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: sdk_paths_id_seq; Type: SEQUENCE; Schema: adtech; Owner: postgres
--

ALTER TABLE adtech.sdk_paths ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME adtech.sdk_paths_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: sdks_id_seq; Type: SEQUENCE; Schema: adtech; Owner: postgres
--

ALTER TABLE adtech.sdks ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME adtech.sdks_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: url_redirect_chains; Type: TABLE; Schema: adtech; Owner: postgres
--

CREATE TABLE adtech.url_redirect_chains (
    id integer NOT NULL,
    run_id integer NOT NULL,
    api_call_id integer NOT NULL,
    url_id integer NOT NULL,
    next_url_id integer NOT NULL,
    hop_index integer NOT NULL,
    is_chain_start boolean DEFAULT false,
    is_chain_end boolean DEFAULT false,
    created_at timestamp with time zone DEFAULT now()
);


ALTER TABLE adtech.url_redirect_chains OWNER TO postgres;

--
-- Name: url_redirect_chains_id_seq; Type: SEQUENCE; Schema: adtech; Owner: postgres
--

CREATE SEQUENCE adtech.url_redirect_chains_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE adtech.url_redirect_chains_id_seq OWNER TO postgres;

--
-- Name: url_redirect_chains_id_seq; Type: SEQUENCE OWNED BY; Schema: adtech; Owner: postgres
--

ALTER SEQUENCE adtech.url_redirect_chains_id_seq OWNED BY adtech.url_redirect_chains.id;


--
-- Name: urls; Type: TABLE; Schema: adtech; Owner: postgres
--

CREATE TABLE adtech.urls (
    id integer NOT NULL,
    url text NOT NULL,
    domain_id integer,
    scheme text NOT NULL,
    is_deep_link boolean GENERATED ALWAYS AS ((scheme <> ALL (ARRAY['http'::text, 'https'::text, 'ftp'::text]))) STORED,
    created_at timestamp with time zone DEFAULT now(),
    hostname text,
    url_hash character(32)
);


ALTER TABLE adtech.urls OWNER TO postgres;

--
-- Name: urls_id_seq; Type: SEQUENCE; Schema: adtech; Owner: postgres
--

CREATE SEQUENCE adtech.urls_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE adtech.urls_id_seq OWNER TO postgres;

--
-- Name: urls_id_seq; Type: SEQUENCE OWNED BY; Schema: adtech; Owner: postgres
--

ALTER SEQUENCE adtech.urls_id_seq OWNED BY adtech.urls.id;


--
-- Name: adstxt_entries_store_apps; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.adstxt_entries_store_apps AS
 WITH parent_companies AS (
         SELECT c.id AS company_id,
            c.name AS company_name,
            COALESCE(c.parent_company_id, c.id) AS parent_company_id,
            COALESCE(pc.name, c.name) AS parent_company_name,
            COALESCE(pc.domain_id, c.domain_id) AS parent_company_domain_id
           FROM (adtech.companies c
             LEFT JOIN adtech.companies pc ON ((c.parent_company_id = pc.id)))
        )
 SELECT DISTINCT ad.id AS ad_domain_id,
    myc.parent_company_id AS company_id,
    aae.id AS app_ad_entry_id,
    sa.id AS store_app,
    pd.id AS pub_domain_id
   FROM (((((((public.app_ads_entrys aae
     LEFT JOIN public.domains ad ON ((aae.ad_domain = ad.id)))
     LEFT JOIN public.app_ads_map aam ON ((aae.id = aam.app_ads_entry)))
     LEFT JOIN public.domains pd ON ((aam.pub_domain = pd.id)))
     LEFT JOIN public.app_urls_map aum ON ((pd.id = aum.pub_domain)))
     JOIN public.store_apps sa ON ((aum.store_app = sa.id)))
     LEFT JOIN parent_companies myc ON ((ad.id = myc.parent_company_domain_id)))
     LEFT JOIN public.adstxt_crawl_results pdcr ON ((pd.id = pdcr.domain_id)))
  WHERE ((pdcr.crawled_at - aam.updated_at) < '01:00:00'::interval)
  WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.adstxt_entries_store_apps OWNER TO postgres;

--
-- Name: adstxt_ad_domain_overview; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.adstxt_ad_domain_overview AS
 SELECT ad.domain_name AS ad_domain_url,
    aae.relationship,
    sa.store,
    count(DISTINCT aae.publisher_id) AS publisher_id_count,
    count(DISTINCT sa.developer) AS developer_count,
    count(DISTINCT aesa.store_app) AS app_count
   FROM (((frontend.adstxt_entries_store_apps aesa
     LEFT JOIN public.store_apps sa ON ((aesa.store_app = sa.id)))
     LEFT JOIN public.app_ads_entrys aae ON ((aesa.app_ad_entry_id = aae.id)))
     LEFT JOIN public.domains ad ON ((aesa.ad_domain_id = ad.id)))
  GROUP BY ad.domain_name, aae.relationship, sa.store
  WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.adstxt_ad_domain_overview OWNER TO postgres;

--
-- Name: adstxt_publishers_overview; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.adstxt_publishers_overview AS
 WITH ranked_data AS (
         SELECT ad.domain_name AS ad_domain_url,
            aae.relationship,
            sa.store,
            aae.publisher_id,
            count(DISTINCT sa.developer) AS developer_count,
            count(DISTINCT aesa.store_app) AS app_count,
            row_number() OVER (PARTITION BY ad.domain_name, aae.relationship, sa.store ORDER BY (count(DISTINCT aesa.store_app)) DESC) AS pubrank
           FROM (((frontend.adstxt_entries_store_apps aesa
             LEFT JOIN public.store_apps sa ON ((aesa.store_app = sa.id)))
             LEFT JOIN public.app_ads_entrys aae ON ((aesa.app_ad_entry_id = aae.id)))
             LEFT JOIN public.domains ad ON ((aesa.ad_domain_id = ad.id)))
          GROUP BY ad.domain_name, aae.relationship, sa.store, aae.publisher_id
        )
 SELECT ad_domain_url,
    relationship,
    store,
    publisher_id,
    developer_count,
    app_count,
    pubrank
   FROM ranked_data
  WHERE (pubrank <= 50)
  WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.adstxt_publishers_overview OWNER TO postgres;

--
-- Name: creative_assets; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.creative_assets (
    id integer NOT NULL,
    md5_hash character varying NOT NULL,
    file_extension character varying NOT NULL,
    phash character varying,
    created_at timestamp without time zone DEFAULT timezone('utc'::text, now()) NOT NULL
);


ALTER TABLE public.creative_assets OWNER TO postgres;

--
-- Name: advertiser_creative_rankings; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.advertiser_creative_rankings AS
 WITH adv_mmp AS (
         SELECT DISTINCT cr_1.advertiser_store_app_id,
            cr_1.mmp_domain_id,
            ad.domain_name AS mmp_domain
           FROM (public.creative_records cr_1
             LEFT JOIN public.domains ad ON ((cr_1.mmp_domain_id = ad.id)))
          WHERE ((cr_1.mmp_domain_id IS NOT NULL) AND (cr_1.advertiser_store_app_id IS NOT NULL))
        ), ad_network_domain_ids AS (
         SELECT cr_1.advertiser_store_app_id,
            COALESCE(icp.domain_id, ic.domain_id) AS domain_id
           FROM (((public.creative_records cr_1
             JOIN adtech.company_domain_mapping icdm ON ((cr_1.creative_initial_domain_id = icdm.domain_id)))
             LEFT JOIN adtech.companies ic ON ((icdm.company_id = ic.id)))
             LEFT JOIN adtech.companies icp ON ((ic.parent_company_id = icp.id)))
        UNION
         SELECT cr_1.advertiser_store_app_id,
            COALESCE(hcp.domain_id, hc.domain_id) AS domain_id
           FROM (((public.creative_records cr_1
             JOIN adtech.company_domain_mapping hcdm ON ((cr_1.creative_host_domain_id = hcdm.domain_id)))
             LEFT JOIN adtech.companies hc ON ((hcdm.company_id = hc.id)))
             LEFT JOIN adtech.companies hcp ON ((hc.parent_company_id = hcp.id)))
        ), ad_network_domains AS (
         SELECT adi.advertiser_store_app_id,
            ad.domain_name AS ad_network_domain
           FROM (ad_network_domain_ids adi
             LEFT JOIN public.domains ad ON ((adi.domain_id = ad.id)))
        ), creative_rankings AS (
         SELECT ca_1.md5_hash,
            ca_1.file_extension,
            cr_1.advertiser_store_app_id,
            vcasr_1.run_at,
            row_number() OVER (PARTITION BY cr_1.advertiser_store_app_id ORDER BY vcasr_1.run_at DESC) AS rn
           FROM (((public.creative_records cr_1
             LEFT JOIN public.creative_assets ca_1 ON ((cr_1.creative_asset_id = ca_1.id)))
             LEFT JOIN public.api_calls ac_1 ON ((cr_1.api_call_id = ac_1.id)))
             LEFT JOIN public.version_code_api_scan_results vcasr_1 ON ((ac_1.run_id = vcasr_1.id)))
        )
 SELECT saa.name AS advertiser_name,
    saa.store_id AS advertiser_store_id,
    saa.icon_url_100 AS advertiser_icon_url_100,
    saa.icon_url_512 AS advertiser_icon_url_512,
    saa.category AS advertiser_category,
    saa.installs AS advertiser_installs,
    saa.rating,
    saa.rating_count,
    saa.installs_sum_1w,
    saa.installs_sum_4w,
    count(DISTINCT ca.md5_hash) AS unique_creatives,
    count(DISTINCT ac.store_app) AS unique_publishers,
    min(vcasr.run_at) AS first_seen,
    max(vcasr.run_at) AS last_seen,
    array_agg(DISTINCT ca.file_extension) AS file_types,
    array_agg(DISTINCT adis.ad_network_domain) AS ad_network_domains,
    avg(sap.installs) AS avg_publisher_installs,
    NULLIF(array_agg(DISTINCT adv_mmp.mmp_domain) FILTER (WHERE (adv_mmp.mmp_domain IS NOT NULL)), '{}'::character varying[]) AS mmp_domains,
    ARRAY( SELECT crk.md5_hash
           FROM creative_rankings crk
          WHERE ((crk.advertiser_store_app_id = saa.id) AND (crk.rn <= 5))
          ORDER BY crk.rn) AS top_md5_hashes
   FROM (((((((public.creative_records cr
     LEFT JOIN public.creative_assets ca ON ((cr.creative_asset_id = ca.id)))
     LEFT JOIN public.api_calls ac ON ((cr.api_call_id = ac.id)))
     LEFT JOIN frontend.store_apps_overview sap ON ((ac.store_app = sap.id)))
     LEFT JOIN frontend.store_apps_overview saa ON ((cr.advertiser_store_app_id = saa.id)))
     LEFT JOIN public.version_code_api_scan_results vcasr ON ((ac.run_id = vcasr.id)))
     LEFT JOIN adv_mmp ON ((cr.advertiser_store_app_id = adv_mmp.advertiser_store_app_id)))
     LEFT JOIN ad_network_domains adis ON ((cr.advertiser_store_app_id = adis.advertiser_store_app_id)))
  WHERE (cr.advertiser_store_app_id IS NOT NULL)
  GROUP BY saa.name, saa.store_id, saa.icon_url_512, saa.category, saa.installs, saa.id, saa.icon_url_100, saa.rating, saa.rating_count, saa.installs_sum_1w, saa.installs_sum_4w
  ORDER BY (count(DISTINCT ca.md5_hash)) DESC
  WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.advertiser_creative_rankings OWNER TO postgres;

--
-- Name: advertiser_creative_rankings_recent_month; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.advertiser_creative_rankings_recent_month AS
 WITH adv_mmp AS (
         SELECT DISTINCT cr_1.advertiser_store_app_id,
            cr_1.mmp_domain_id,
            ad.domain_name AS mmp_domain
           FROM (public.creative_records cr_1
             LEFT JOIN public.domains ad ON (((cr_1.mmp_domain_id = ad.id) AND (cr_1.advertiser_store_app_id IS NOT NULL))))
          WHERE (cr_1.mmp_domain_id IS NOT NULL)
        ), ad_network_domain_ids AS (
         SELECT cr_1.advertiser_store_app_id,
            COALESCE(icp.domain_id, ic.domain_id) AS domain_id
           FROM (((((public.creative_records cr_1
             JOIN adtech.company_domain_mapping icdm ON ((cr_1.creative_initial_domain_id = icdm.domain_id)))
             LEFT JOIN adtech.companies ic ON ((icdm.company_id = ic.id)))
             LEFT JOIN adtech.companies icp ON ((ic.parent_company_id = icp.id)))
             LEFT JOIN public.api_calls ac_1 ON ((cr_1.api_call_id = ac_1.id)))
             LEFT JOIN public.version_code_api_scan_results vcasr_1 ON ((ac_1.run_id = vcasr_1.id)))
          WHERE (vcasr_1.run_at >= (now() - '1 mon'::interval))
        UNION
         SELECT cr_1.advertiser_store_app_id,
            COALESCE(hcp.domain_id, hc.domain_id) AS domain_id
           FROM (((((public.creative_records cr_1
             JOIN adtech.company_domain_mapping hcdm ON ((cr_1.creative_host_domain_id = hcdm.domain_id)))
             LEFT JOIN adtech.companies hc ON ((hcdm.company_id = hc.id)))
             LEFT JOIN adtech.companies hcp ON ((hc.parent_company_id = hcp.id)))
             LEFT JOIN public.api_calls ac_1 ON ((cr_1.api_call_id = ac_1.id)))
             LEFT JOIN public.version_code_api_scan_results vcasr_1 ON ((ac_1.run_id = vcasr_1.id)))
          WHERE (vcasr_1.run_at >= (now() - '1 mon'::interval))
        ), ad_network_domains AS (
         SELECT adi.advertiser_store_app_id,
            ad.domain_name AS ad_network_domain
           FROM (ad_network_domain_ids adi
             LEFT JOIN public.domains ad ON ((adi.domain_id = ad.id)))
        ), creative_rankings AS (
         SELECT ca_1.md5_hash,
            ca_1.file_extension,
            cr_1.advertiser_store_app_id,
            vcasr_1.run_at,
            row_number() OVER (PARTITION BY cr_1.advertiser_store_app_id ORDER BY vcasr_1.run_at DESC) AS rn
           FROM (((public.creative_records cr_1
             LEFT JOIN public.creative_assets ca_1 ON ((cr_1.creative_asset_id = ca_1.id)))
             LEFT JOIN public.api_calls ac_1 ON ((cr_1.api_call_id = ac_1.id)))
             LEFT JOIN public.version_code_api_scan_results vcasr_1 ON ((ac_1.run_id = vcasr_1.id)))
          WHERE (vcasr_1.run_at >= (now() - '1 mon'::interval))
        )
 SELECT saa.name AS advertiser_name,
    saa.store_id AS advertiser_store_id,
    saa.icon_url_100 AS advertiser_icon_url_100,
    saa.icon_url_512 AS advertiser_icon_url_512,
    saa.category AS advertiser_category,
    saa.installs AS advertiser_installs,
    saa.rating,
    saa.rating_count,
    saa.installs_sum_1w,
    saa.installs_sum_4w,
    count(DISTINCT ca.md5_hash) AS unique_creatives,
    count(DISTINCT ac.store_app) AS unique_publishers,
    min(vcasr.run_at) AS first_seen,
    max(vcasr.run_at) AS last_seen,
    array_agg(DISTINCT ca.file_extension) AS file_types,
    array_agg(DISTINCT adis.ad_network_domain) AS ad_network_domains,
    avg(sap.installs) AS avg_publisher_installs,
    NULLIF(array_agg(DISTINCT adv_mmp.mmp_domain) FILTER (WHERE (adv_mmp.mmp_domain IS NOT NULL)), '{}'::character varying[]) AS mmp_domains,
    ARRAY( SELECT crk.md5_hash
           FROM creative_rankings crk
          WHERE ((crk.advertiser_store_app_id = saa.id) AND (crk.rn <= 5))
          ORDER BY crk.rn) AS top_md5_hashes
   FROM (((((((public.creative_records cr
     LEFT JOIN public.api_calls ac ON ((cr.api_call_id = ac.id)))
     LEFT JOIN public.creative_assets ca ON ((cr.creative_asset_id = ca.id)))
     LEFT JOIN frontend.store_apps_overview sap ON ((ac.store_app = sap.id)))
     LEFT JOIN frontend.store_apps_overview saa ON ((cr.advertiser_store_app_id = saa.id)))
     LEFT JOIN public.version_code_api_scan_results vcasr ON ((ac.run_id = vcasr.id)))
     LEFT JOIN adv_mmp ON ((cr.advertiser_store_app_id = adv_mmp.advertiser_store_app_id)))
     LEFT JOIN ad_network_domains adis ON ((cr.advertiser_store_app_id = adis.advertiser_store_app_id)))
  WHERE ((vcasr.run_at >= (now() - '1 mon'::interval)) AND (cr.advertiser_store_app_id IS NOT NULL))
  GROUP BY saa.name, saa.store_id, saa.icon_url_512, saa.category, saa.installs, saa.id, saa.icon_url_100, saa.rating, saa.rating_count, saa.installs_sum_1w, saa.installs_sum_4w
  ORDER BY (count(DISTINCT ca.md5_hash)) DESC
  WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.advertiser_creative_rankings_recent_month OWNER TO postgres;

--
-- Name: advertiser_creatives; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.advertiser_creatives AS
 SELECT saa.store_id AS advertiser_store_id,
    ac1.run_id,
    vcasr.run_at,
    sap.name AS pub_name,
    saa.name AS adv_name,
    sap.store_id AS pub_store_id,
    saa.store_id AS adv_store_id,
    hd.domain_name AS host_domain,
    hc.name AS host_domain_company_name,
    ad.domain_name AS ad_domain,
    ac.name AS ad_domain_company_name,
    ca.md5_hash,
    ca.file_extension,
    sap.icon_url_100 AS pub_icon_url_100,
    saa.icon_url_100 AS adv_icon_url_100,
    sap.icon_url_512 AS pub_icon_url_512,
    saa.icon_url_512 AS adv_icon_url_512,
    mmp.name AS mmp_name,
    mmpd.domain_name AS mmp_domain,
    cr.mmp_urls,
    COALESCE(hcd.domain_name, hd.domain_name) AS host_domain_company_domain,
    COALESCE(acd.domain_name, ad.domain_name) AS ad_domain_company_domain,
    COALESCE(ca.phash, ca.md5_hash) AS vhash,
    ( SELECT COALESCE(array_agg(domains.domain_name), '{}'::character varying[]) AS array_agg
           FROM public.domains
          WHERE (domains.id = ANY (cr.additional_ad_domain_ids))) AS additional_ad_domain_urls
   FROM ((((((((((((((((public.creative_records cr
     LEFT JOIN public.creative_assets ca ON ((cr.creative_asset_id = ca.id)))
     LEFT JOIN public.api_calls ac1 ON ((cr.api_call_id = ac1.id)))
     LEFT JOIN frontend.store_apps_overview sap ON ((ac1.store_app = sap.id)))
     LEFT JOIN frontend.store_apps_overview saa ON ((cr.advertiser_store_app_id = saa.id)))
     LEFT JOIN public.version_code_api_scan_results vcasr ON ((ac1.run_id = vcasr.id)))
     LEFT JOIN public.domains hd ON ((cr.creative_host_domain_id = hd.id)))
     LEFT JOIN public.domains ad ON ((cr.creative_initial_domain_id = ad.id)))
     LEFT JOIN adtech.company_domain_mapping hcdm ON ((hd.id = hcdm.domain_id)))
     LEFT JOIN adtech.company_domain_mapping acdm ON ((ad.id = acdm.domain_id)))
     LEFT JOIN adtech.companies hc ON ((hcdm.company_id = hc.id)))
     LEFT JOIN adtech.companies ac ON ((acdm.company_id = ac.id)))
     LEFT JOIN public.domains hcd ON ((hc.domain_id = hcd.id)))
     LEFT JOIN public.domains acd ON ((ac.domain_id = acd.id)))
     LEFT JOIN adtech.company_domain_mapping cdm ON ((cr.mmp_domain_id = cdm.domain_id)))
     LEFT JOIN adtech.companies mmp ON ((cdm.company_id = mmp.id)))
     LEFT JOIN public.domains mmpd ON ((cr.mmp_domain_id = mmpd.id)))
  WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.advertiser_creatives OWNER TO postgres;

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
-- Name: ip_geo_snapshots; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.ip_geo_snapshots (
    id integer NOT NULL,
    mitm_uuid uuid NOT NULL,
    ip_address inet NOT NULL,
    country_id integer,
    state_iso character varying(4),
    city_name character varying,
    org character varying,
    created_at timestamp with time zone DEFAULT timezone('utc'::text, now()) NOT NULL
);


ALTER TABLE public.ip_geo_snapshots OWNER TO postgres;

--
-- Name: api_call_countries; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.api_call_countries AS
 WITH latest_run_per_app AS (
         SELECT DISTINCT ON (ac.store_app) ac.store_app,
            ac.run_id
           FROM ((public.api_calls ac
             JOIN public.version_code_api_scan_results vcasr ON ((ac.run_id = vcasr.id)))
             JOIN public.ip_geo_snapshots igs ON ((ac.ip_geo_snapshot_id = igs.id)))
          WHERE (igs.country_id IS NOT NULL)
          ORDER BY ac.store_app, vcasr.run_at DESC
        ), filtered_calls AS (
         SELECT ac.store_app,
            ac.tld_url,
            ac.url,
            igs.country_id,
            igs.city_name,
            igs.org
           FROM ((public.api_calls ac
             JOIN latest_run_per_app lra ON (((ac.store_app = lra.store_app) AND (ac.run_id = lra.run_id))))
             JOIN public.ip_geo_snapshots igs ON ((ac.ip_geo_snapshot_id = igs.id)))
        ), cleaned_calls AS (
         SELECT filtered_calls.store_app,
            filtered_calls.tld_url,
            filtered_calls.country_id,
            filtered_calls.city_name,
            filtered_calls.org,
            regexp_replace(regexp_replace(regexp_replace(filtered_calls.url, '^https?://'::text, ''::text), '\?.*$'::text, ''::text), '^(([^/]+/){0,2}[^/]+).*$'::text, '\1'::text) AS short_url
           FROM filtered_calls
        )
 SELECT ca.tld_url,
    co.alpha2 AS country,
    ca.org,
    COALESCE(cad.domain_name, (ca.tld_url)::character varying) AS company_domain,
    COALESCE(pcad.domain_name, COALESCE(cad.domain_name, (ca.tld_url)::character varying)) AS parent_company_domain,
    count(DISTINCT ca.store_app) AS store_app_count
   FROM (((((((cleaned_calls ca
     LEFT JOIN public.domains ad ON ((ca.tld_url = (ad.domain_name)::text)))
     LEFT JOIN adtech.company_domain_mapping cdm ON ((ad.id = cdm.domain_id)))
     LEFT JOIN adtech.companies c ON ((cdm.company_id = c.id)))
     LEFT JOIN public.domains cad ON ((c.domain_id = cad.id)))
     LEFT JOIN adtech.companies pc ON ((c.parent_company_id = pc.id)))
     LEFT JOIN public.domains pcad ON ((pc.domain_id = pcad.id)))
     LEFT JOIN public.countries co ON ((ca.country_id = co.id)))
  GROUP BY COALESCE(cad.domain_name, (ca.tld_url)::character varying), COALESCE(pcad.domain_name, COALESCE(cad.domain_name, (ca.tld_url)::character varying)), ca.tld_url, co.alpha2, ca.org
  ORDER BY (count(DISTINCT ca.store_app)) DESC
  WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.api_call_countries OWNER TO postgres;

--
-- Name: app_keyword_ranks_daily; Type: TABLE; Schema: frontend; Owner: postgres
--

CREATE TABLE frontend.app_keyword_ranks_daily (
    crawled_date date NOT NULL,
    store smallint NOT NULL,
    country smallint NOT NULL,
    keyword_id integer NOT NULL,
    store_app integer NOT NULL,
    app_rank smallint NOT NULL
);


ALTER TABLE frontend.app_keyword_ranks_daily OWNER TO postgres;

--
-- Name: app_keyword_rank_stats; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.app_keyword_rank_stats AS
 WITH latest_per_country AS (
         SELECT app_keyword_ranks_daily.country,
            max(app_keyword_ranks_daily.crawled_date) AS max_crawled_date
           FROM frontend.app_keyword_ranks_daily
          GROUP BY app_keyword_ranks_daily.country
        ), d30_keywords AS (
         SELECT akr.country,
            akr.store_app,
            akr.keyword_id,
            min(akr.app_rank) AS d30_best_rank
           FROM frontend.app_keyword_ranks_daily akr
          WHERE (akr.crawled_date >= (CURRENT_DATE - '30 days'::interval))
          GROUP BY akr.country, akr.store_app, akr.keyword_id
        ), latest_ranks AS (
         SELECT kr.country,
            kr.store_app,
            kr.keyword_id,
            kr.app_rank AS latest_app_rank
           FROM (frontend.app_keyword_ranks_daily kr
             JOIN latest_per_country lpc ON (((kr.country = lpc.country) AND (kr.crawled_date = lpc.max_crawled_date))))
        ), all_ranked_keywords AS (
         SELECT rk.country,
            rk.store_app,
            rk.keyword_id,
            rk.d30_best_rank,
            lk.latest_app_rank
           FROM (d30_keywords rk
             LEFT JOIN latest_ranks lk ON (((lk.country = rk.country) AND (lk.store_app = rk.store_app) AND (lk.keyword_id = rk.keyword_id))))
        )
 SELECT country,
    store_app,
    keyword_id,
    d30_best_rank,
    latest_app_rank
   FROM all_ranked_keywords
  WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.app_keyword_rank_stats OWNER TO postgres;

--
-- Name: apps_new_monthly; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.apps_new_monthly AS
 WITH rankedapps AS (
         SELECT sa_1.id,
            sa_1.name,
            sa_1.store_id,
            sa_1.store,
            sa_1.category,
            sa_1.developer_name,
            sa_1.rating,
            sa_1.installs,
            sa_1.installs_sum_1w,
            sa_1.installs_sum_4w,
            sa_1.ratings_sum_1w,
            sa_1.ratings_sum_4w,
            sa_1.store_last_updated,
            sa_1.ad_supported,
            sa_1.in_app_purchases,
            sa_1.created_at,
            sa_1.updated_at,
            sa_1.crawl_result,
            sa_1.icon_url_100,
            sa_1.release_date,
            sa_1.rating_count,
            sa_1.featured_image_url,
            sa_1.phone_image_url_1,
            sa_1.phone_image_url_2,
            sa_1.phone_image_url_3,
            sa_1.tablet_image_url_1,
            sa_1.tablet_image_url_2,
            sa_1.tablet_image_url_3,
            row_number() OVER (PARTITION BY sa_1.store, sa_1.category ORDER BY sa_1.installs DESC NULLS LAST, sa_1.rating_count DESC NULLS LAST) AS rn
           FROM frontend.store_apps_overview sa_1
          WHERE ((sa_1.release_date >= (CURRENT_DATE - '30 days'::interval)) AND (sa_1.created_at >= (CURRENT_DATE - '45 days'::interval)) AND (sa_1.crawl_result = 1))
        )
 SELECT id,
    name,
    store_id,
    store,
    category,
    developer_name,
    rating,
    installs,
    installs_sum_1w,
    installs_sum_4w,
    ratings_sum_1w,
    ratings_sum_4w,
    store_last_updated,
    ad_supported,
    in_app_purchases,
    created_at,
    updated_at,
    crawl_result,
    icon_url_100,
    release_date,
    rating_count,
    featured_image_url,
    phone_image_url_1,
    phone_image_url_2,
    phone_image_url_3,
    tablet_image_url_1,
    tablet_image_url_2,
    tablet_image_url_3,
    category AS app_category,
    rn
   FROM rankedapps ra
  WHERE (rn <= 100)
  WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.apps_new_monthly OWNER TO postgres;

--
-- Name: apps_new_weekly; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.apps_new_weekly AS
 WITH rankedapps AS (
         SELECT sa_1.id,
            sa_1.name,
            sa_1.store_id,
            sa_1.store,
            sa_1.category,
            sa_1.developer_name,
            sa_1.rating,
            sa_1.installs,
            sa_1.installs_sum_1w,
            sa_1.installs_sum_4w,
            sa_1.ratings_sum_1w,
            sa_1.ratings_sum_4w,
            sa_1.store_last_updated,
            sa_1.ad_supported,
            sa_1.in_app_purchases,
            sa_1.created_at,
            sa_1.updated_at,
            sa_1.crawl_result,
            sa_1.icon_url_100,
            sa_1.release_date,
            sa_1.rating_count,
            sa_1.featured_image_url,
            sa_1.phone_image_url_1,
            sa_1.phone_image_url_2,
            sa_1.phone_image_url_3,
            sa_1.tablet_image_url_1,
            sa_1.tablet_image_url_2,
            sa_1.tablet_image_url_3,
            row_number() OVER (PARTITION BY sa_1.store, sa_1.category ORDER BY sa_1.installs DESC NULLS LAST, sa_1.rating_count DESC NULLS LAST) AS rn
           FROM frontend.store_apps_overview sa_1
          WHERE ((sa_1.release_date >= (CURRENT_DATE - '7 days'::interval)) AND (sa_1.created_at >= (CURRENT_DATE - '11 days'::interval)) AND (sa_1.crawl_result = 1))
        )
 SELECT id,
    name,
    store_id,
    store,
    category,
    developer_name,
    rating,
    installs,
    installs_sum_1w,
    installs_sum_4w,
    ratings_sum_1w,
    ratings_sum_4w,
    store_last_updated,
    ad_supported,
    in_app_purchases,
    created_at,
    updated_at,
    crawl_result,
    icon_url_100,
    release_date,
    rating_count,
    featured_image_url,
    phone_image_url_1,
    phone_image_url_2,
    phone_image_url_3,
    tablet_image_url_1,
    tablet_image_url_2,
    tablet_image_url_3,
    category AS app_category,
    rn
   FROM rankedapps ra
  WHERE (rn <= 100)
  WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.apps_new_weekly OWNER TO postgres;

--
-- Name: apps_new_yearly; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.apps_new_yearly AS
 WITH rankedapps AS (
         SELECT sa_1.id,
            sa_1.name,
            sa_1.store_id,
            sa_1.store,
            sa_1.category,
            sa_1.developer_name,
            sa_1.icon_url_100,
            sa_1.rating,
            sa_1.installs,
            sa_1.installs_sum_1w,
            sa_1.installs_sum_4w,
            sa_1.ratings_sum_1w,
            sa_1.ratings_sum_4w,
            sa_1.store_last_updated,
            sa_1.ad_supported,
            sa_1.in_app_purchases,
            sa_1.created_at,
            sa_1.updated_at,
            sa_1.crawl_result,
            sa_1.icon_url_512,
            sa_1.release_date,
            sa_1.rating_count,
            sa_1.featured_image_url,
            sa_1.phone_image_url_1,
            sa_1.phone_image_url_2,
            sa_1.phone_image_url_3,
            sa_1.tablet_image_url_1,
            sa_1.tablet_image_url_2,
            sa_1.tablet_image_url_3,
            row_number() OVER (PARTITION BY sa_1.store, sa_1.category ORDER BY sa_1.installs DESC NULLS LAST, sa_1.rating_count DESC NULLS LAST) AS rn
           FROM frontend.store_apps_overview sa_1
          WHERE ((sa_1.release_date >= (CURRENT_DATE - '365 days'::interval)) AND (sa_1.created_at >= (CURRENT_DATE - '380 days'::interval)) AND (sa_1.crawl_result = 1))
        )
 SELECT id,
    name,
    store_id,
    store,
    category,
    developer_name,
    rating,
    installs,
    installs_sum_1w,
    installs_sum_4w,
    ratings_sum_1w,
    ratings_sum_4w,
    store_last_updated,
    ad_supported,
    in_app_purchases,
    created_at,
    updated_at,
    crawl_result,
    icon_url_100,
    release_date,
    rating_count,
    featured_image_url,
    phone_image_url_1,
    phone_image_url_2,
    phone_image_url_3,
    tablet_image_url_1,
    tablet_image_url_2,
    tablet_image_url_3,
    category AS app_category,
    rn
   FROM rankedapps ra
  WHERE (rn <= 100)
  WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.apps_new_yearly OWNER TO postgres;

--
-- Name: category_tag_stats; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.category_tag_stats AS
 WITH d30_counts AS (
         SELECT sahw.store_app,
            sum(sahw.installs_diff) AS d30_installs,
            sum(sahw.rating_count_diff) AS d30_rating_count
           FROM public.app_global_metrics_weekly_diffs sahw
          WHERE ((sahw.week_start > (CURRENT_DATE - '31 days'::interval)) AND ((sahw.installs_diff > (0)::numeric) OR (sahw.rating_count_diff > (0)::numeric)))
          GROUP BY sahw.store_app
        ), distinct_apps_group AS (
         SELECT sa.store,
            csac.store_app,
            csac.app_category,
            tag.tag_source,
            sa.installs,
            sa.rating_count
           FROM ((adtech.combined_store_apps_companies csac
             LEFT JOIN frontend.store_apps_overview sa ON ((csac.store_app = sa.id)))
             CROSS JOIN LATERAL ( VALUES ('sdk'::text,csac.sdk), ('api_call'::text,csac.api_call), ('app_ads_direct'::text,csac.app_ads_direct), ('app_ads_reseller'::text,csac.app_ads_reseller)) tag(tag_source, present))
          WHERE (tag.present IS TRUE)
        )
 SELECT dag.store,
    dag.app_category,
    dag.tag_source,
    count(DISTINCT dag.store_app) AS app_count,
    sum(dc.d30_installs) AS installs_d30,
    sum(dc.d30_rating_count) AS rating_count_d30,
    sum(dag.installs) AS installs_total,
    sum(dag.rating_count) AS rating_count_total
   FROM (distinct_apps_group dag
     LEFT JOIN d30_counts dc ON ((dag.store_app = dc.store_app)))
  GROUP BY dag.store, dag.app_category, dag.tag_source
  WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.category_tag_stats OWNER TO postgres;

--
-- Name: companies_apps_overview; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.companies_apps_overview AS
 WITH store_app_sdk_companies AS (
         SELECT DISTINCT savs.store_app,
            sd.company_id
           FROM (adtech.store_app_sdk_strings savs
             LEFT JOIN adtech.sdks sd ON ((savs.sdk_id = sd.id)))
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
-- Name: companies_category_stats; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.companies_category_stats AS
 WITH d30_counts AS (
         SELECT sahw.store_app,
            sum(sahw.installs_diff) AS d30_installs,
            sum(sahw.rating_count_diff) AS d30_rating_count
           FROM public.app_global_metrics_weekly_diffs sahw
          WHERE ((sahw.week_start > (CURRENT_DATE - '31 days'::interval)) AND ((sahw.installs_diff > (0)::numeric) OR (sahw.rating_count_diff > (0)::numeric)))
          GROUP BY sahw.store_app
        ), distinct_apps_group AS (
         SELECT sa.store,
            csac.store_app,
            csac.app_category,
            csac.ad_domain AS company_domain,
            c.name AS company_name,
            sa.installs,
            sa.rating_count
           FROM ((adtech.combined_store_apps_companies csac
             LEFT JOIN adtech.companies c ON ((csac.company_id = c.id)))
             LEFT JOIN frontend.store_apps_overview sa ON ((csac.store_app = sa.id)))
        )
 SELECT dag.store,
    dag.app_category,
    dag.company_domain,
    dag.company_name,
    count(DISTINCT dag.store_app) AS app_count,
    sum(dc.d30_installs) AS installs_d30,
    sum(dc.d30_rating_count) AS rating_count_d30,
    sum(dag.installs) AS installs_total,
    sum(dag.rating_count) AS rating_count_total
   FROM (distinct_apps_group dag
     LEFT JOIN d30_counts dc ON ((dag.store_app = dc.store_app)))
  GROUP BY dag.store, dag.app_category, dag.company_domain, dag.company_name
  WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.companies_category_stats OWNER TO postgres;

--
-- Name: companies_category_tag_stats; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.companies_category_tag_stats AS
 WITH d30_counts AS (
         SELECT sahw.store_app,
            sum(sahw.installs_diff) AS d30_installs,
            sum(sahw.rating_count_diff) AS d30_rating_count
           FROM public.app_global_metrics_weekly_diffs sahw
          WHERE ((sahw.week_start > (CURRENT_DATE - '31 days'::interval)) AND ((sahw.installs_diff > (0)::numeric) OR (sahw.rating_count_diff > (0)::numeric)))
          GROUP BY sahw.store_app
        ), distinct_apps_group AS (
         SELECT sa.store,
            csac.store_app,
            csac.app_category,
            tag.tag_source,
            csac.ad_domain AS company_domain,
            c.name AS company_name,
            sa.installs,
            sa.rating_count
           FROM (((adtech.combined_store_apps_companies csac
             LEFT JOIN adtech.companies c ON ((csac.company_id = c.id)))
             LEFT JOIN frontend.store_apps_overview sa ON ((csac.store_app = sa.id)))
             CROSS JOIN LATERAL ( VALUES ('sdk'::text,csac.sdk), ('api_call'::text,csac.api_call), ('app_ads_direct'::text,csac.app_ads_direct), ('app_ads_reseller'::text,csac.app_ads_reseller)) tag(tag_source, present))
          WHERE (tag.present IS TRUE)
        )
 SELECT dag.store,
    dag.app_category,
    dag.tag_source,
    dag.company_domain,
    dag.company_name,
    count(DISTINCT dag.store_app) AS app_count,
    sum(dc.d30_installs) AS installs_d30,
    sum(dc.d30_rating_count) AS rating_count_d30,
    sum(dag.installs) AS installs_total,
    sum(dag.rating_count) AS rating_count_total
   FROM (distinct_apps_group dag
     LEFT JOIN d30_counts dc ON ((dag.store_app = dc.store_app)))
  GROUP BY dag.store, dag.app_category, dag.tag_source, dag.company_domain, dag.company_name
  WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.companies_category_tag_stats OWNER TO postgres;

--
-- Name: companies_category_tag_type_stats; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.companies_category_tag_type_stats AS
 WITH d30_counts AS (
         SELECT sahw.store_app,
            sum(sahw.installs_diff) AS d30_installs,
            sum(sahw.rating_count_diff) AS d30_rating_count
           FROM public.app_global_metrics_weekly_diffs sahw
          WHERE ((sahw.week_start > (CURRENT_DATE - '31 days'::interval)) AND ((sahw.installs_diff > (0)::numeric) OR (sahw.rating_count_diff > (0)::numeric)))
          GROUP BY sahw.store_app
        ), minimized_company_categories AS (
         SELECT company_categories.company_id,
            min(company_categories.category_id) AS category_id
           FROM adtech.company_categories
          GROUP BY company_categories.company_id
        ), api_and_app_ads AS (
         SELECT sa.store,
            csac.app_category,
            tag.tag_source,
            csac.ad_domain AS company_domain,
            c.name AS company_name,
                CASE
                    WHEN (tag.tag_source ~~ 'app_ads%'::text) THEN 'ad-networks'::character varying
                    ELSE cats.url_slug
                END AS type_url_slug,
            count(DISTINCT csac.store_app) AS app_count,
            sum(dc.d30_installs) AS installs_d30,
            sum(dc.d30_rating_count) AS rating_count_d30,
            sum(sa.installs) AS installs_total,
            sum(sa.rating_count) AS rating_count_total
           FROM ((((((adtech.combined_store_apps_companies csac
             LEFT JOIN adtech.companies c ON ((csac.company_id = c.id)))
             LEFT JOIN frontend.store_apps_overview sa ON ((csac.store_app = sa.id)))
             LEFT JOIN d30_counts dc ON ((csac.store_app = dc.store_app)))
             LEFT JOIN minimized_company_categories mcc ON ((csac.company_id = mcc.company_id)))
             LEFT JOIN adtech.categories cats ON ((mcc.category_id = cats.id)))
             CROSS JOIN LATERAL ( VALUES ('api_call'::text,csac.api_call), ('app_ads_direct'::text,csac.app_ads_direct), ('app_ads_reseller'::text,csac.app_ads_reseller)) tag(tag_source, present))
          WHERE (tag.present IS TRUE)
          GROUP BY sa.store, csac.app_category, tag.tag_source, csac.ad_domain, c.name,
                CASE
                    WHEN (tag.tag_source ~~ 'app_ads%'::text) THEN 'ad-networks'::character varying
                    ELSE cats.url_slug
                END
        ), store_app_sdks AS (
         SELECT DISTINCT sass.store_app,
            sass.sdk_id
           FROM adtech.store_app_sdk_strings sass
          WHERE (sass.sdk_id IS NOT NULL)
        ), sdk_and_mediation AS (
         SELECT sa.store,
            sa.category AS app_category,
            'sdk'::text AS tag_source,
            d.domain_name AS company_domain,
            c.name AS company_name,
            cats.url_slug AS type_url_slug,
            count(DISTINCT sas.store_app) AS app_count,
            sum(dc.d30_installs) AS installs_d30,
            sum(dc.d30_rating_count) AS rating_count_d30,
            sum(sa.installs) AS installs_total,
            sum(sa.rating_count) AS rating_count_total
           FROM (((((((store_app_sdks sas
             LEFT JOIN adtech.sdks s ON ((sas.sdk_id = s.id)))
             LEFT JOIN adtech.companies c ON ((s.company_id = c.id)))
             LEFT JOIN public.domains d ON ((c.domain_id = d.id)))
             LEFT JOIN frontend.store_apps_overview sa ON ((sas.store_app = sa.id)))
             LEFT JOIN d30_counts dc ON ((sas.store_app = dc.store_app)))
             LEFT JOIN adtech.sdk_categories sc ON ((sas.sdk_id = sc.sdk_id)))
             LEFT JOIN adtech.categories cats ON ((sc.category_id = cats.id)))
          GROUP BY sa.store, sa.category, 'sdk'::text, d.domain_name, c.name, cats.url_slug
        )
 SELECT api_and_app_ads.store,
    api_and_app_ads.app_category,
    api_and_app_ads.tag_source,
    api_and_app_ads.company_domain,
    api_and_app_ads.company_name,
    api_and_app_ads.type_url_slug,
    api_and_app_ads.app_count,
    api_and_app_ads.installs_d30,
    api_and_app_ads.rating_count_d30,
    api_and_app_ads.installs_total,
    api_and_app_ads.rating_count_total
   FROM api_and_app_ads
UNION ALL
 SELECT sdk_and_mediation.store,
    sdk_and_mediation.app_category,
    sdk_and_mediation.tag_source,
    sdk_and_mediation.company_domain,
    sdk_and_mediation.company_name,
    sdk_and_mediation.type_url_slug,
    sdk_and_mediation.app_count,
    sdk_and_mediation.installs_d30,
    sdk_and_mediation.rating_count_d30,
    sdk_and_mediation.installs_total,
    sdk_and_mediation.rating_count_total
   FROM sdk_and_mediation
  WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.companies_category_tag_type_stats OWNER TO postgres;

--
-- Name: companies_creative_rankings; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.companies_creative_rankings AS
 WITH creative_rankings AS (
         SELECT ca.file_extension,
            ac_1.id AS api_call_id,
            cr.advertiser_store_app_id,
            cr.advertiser_domain_id,
            cr.creative_initial_domain_id,
            cr.creative_host_domain_id,
            cr.additional_ad_domain_ids,
            vcasr.run_at,
            ca.md5_hash,
            COALESCE(ca.phash, ca.md5_hash) AS vhash
           FROM (((public.creative_records cr
             LEFT JOIN public.creative_assets ca ON ((cr.creative_asset_id = ca.id)))
             LEFT JOIN public.api_calls ac_1 ON ((cr.api_call_id = ac_1.id)))
             LEFT JOIN public.version_code_api_scan_results vcasr ON ((ac_1.run_id = vcasr.id)))
        ), combined_domains AS (
         SELECT cr.api_call_id,
            cr.vhash,
            cr.md5_hash,
            cr.file_extension,
            cr.creative_initial_domain_id AS domain_id,
            cr.advertiser_store_app_id,
            cr.advertiser_domain_id,
            cr.run_at
           FROM creative_rankings cr
        UNION
         SELECT cr.api_call_id,
            cr.vhash,
            cr.md5_hash,
            cr.file_extension,
            cr.creative_host_domain_id,
            cr.advertiser_store_app_id,
            cr.advertiser_domain_id,
            cr.run_at
           FROM creative_rankings cr
        UNION
         SELECT cr.api_call_id,
            cr.vhash,
            cr.md5_hash,
            cr.file_extension,
            unnest(cr.additional_ad_domain_ids) AS unnest,
            cr.advertiser_store_app_id,
            cr.advertiser_domain_id,
            cr.run_at
           FROM creative_rankings cr
        ), visually_distinct AS (
         SELECT cdm.company_id,
            cd.file_extension,
            cd.advertiser_store_app_id,
            cd.advertiser_domain_id,
            cd.vhash,
            min((cd.md5_hash)::text) AS md5_hash,
            max(cd.api_call_id) AS last_api_call_id,
            max(cd.run_at) AS last_seen
           FROM (combined_domains cd
             LEFT JOIN adtech.company_domain_mapping cdm ON ((cd.domain_id = cdm.domain_id)))
          GROUP BY cdm.company_id, cd.file_extension, cd.advertiser_store_app_id, cd.advertiser_domain_id, cd.vhash
        )
 SELECT vd.company_id,
    vd.md5_hash,
    vd.file_extension,
    ad.domain_name AS company_domain,
    saa.name AS advertiser_name,
    saa.store,
    saa.store_id AS advertiser_store_id,
    adv.domain_name AS advertiser_domain_name,
    sap.store_id AS publisher_store_id,
    sap.name AS publisher_name,
    saa.installs,
    saa.rating_count,
    saa.rating,
    saa.installs_sum_1w,
    saa.ratings_sum_1w,
    saa.installs_sum_4w,
    saa.ratings_sum_4w,
    vd.last_seen,
        CASE
            WHEN (saa.icon_url_100 IS NOT NULL) THEN (concat('https://media.appgoblin.info/app-icons/', saa.store_id, '/', saa.icon_url_100))::character varying
            ELSE saa.icon_url_512
        END AS advertiser_icon_url,
        CASE
            WHEN (sap.icon_url_100 IS NOT NULL) THEN (concat('https://media.appgoblin.info/app-icons/', sap.store_id, '/', sap.icon_url_100))::character varying
            ELSE sap.icon_url_512
        END AS publisher_icon_url
   FROM ((((((visually_distinct vd
     LEFT JOIN public.api_calls ac ON ((vd.last_api_call_id = ac.id)))
     LEFT JOIN adtech.companies c ON ((vd.company_id = c.id)))
     LEFT JOIN public.domains ad ON ((c.domain_id = ad.id)))
     LEFT JOIN public.domains adv ON ((vd.advertiser_domain_id = adv.id)))
     LEFT JOIN frontend.store_apps_overview saa ON ((vd.advertiser_store_app_id = saa.id)))
     LEFT JOIN frontend.store_apps_overview sap ON ((ac.store_app = sap.id)))
  WHERE (c.id IS NOT NULL)
  ORDER BY vd.last_seen DESC
  WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.companies_creative_rankings OWNER TO postgres;

--
-- Name: companies_open_source_percent; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.companies_open_source_percent AS
 SELECT ad.domain_name AS company_domain,
    avg(
        CASE
            WHEN sd.is_open_source THEN 1
            ELSE 0
        END) AS percent_open_source
   FROM ((adtech.sdks sd
     LEFT JOIN adtech.companies c ON ((sd.company_id = c.id)))
     LEFT JOIN public.domains ad ON ((c.domain_id = ad.id)))
  GROUP BY ad.domain_name
  WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.companies_open_source_percent OWNER TO postgres;

--
-- Name: companies_parent_category_stats; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.companies_parent_category_stats AS
 WITH d30_counts AS (
         SELECT sahw.store_app,
            sum(sahw.installs_diff) AS d30_installs,
            sum(sahw.rating_count_diff) AS d30_rating_count
           FROM public.app_global_metrics_weekly_diffs sahw
          WHERE ((sahw.week_start > (CURRENT_DATE - '31 days'::interval)) AND ((sahw.installs_diff > (0)::numeric) OR (sahw.rating_count_diff > (0)::numeric)))
          GROUP BY sahw.store_app
        ), distinct_apps_group AS (
         SELECT sa.store,
            csac.store_app,
            csac.app_category,
            c.name AS company_name,
            sa.installs,
            sa.rating_count,
            COALESCE(ad.domain_name, csac.ad_domain) AS company_domain
           FROM (((adtech.combined_store_apps_companies csac
             LEFT JOIN adtech.companies c ON ((csac.parent_id = c.id)))
             LEFT JOIN public.domains ad ON ((c.domain_id = ad.id)))
             LEFT JOIN frontend.store_apps_overview sa ON ((csac.store_app = sa.id)))
          WHERE (csac.parent_id IN ( SELECT DISTINCT pc.id
                   FROM (adtech.companies pc
                     LEFT JOIN adtech.companies c_1 ON ((pc.id = c_1.parent_company_id)))
                  WHERE (c_1.id IS NOT NULL)))
        )
 SELECT dag.store,
    dag.app_category,
    dag.company_domain,
    dag.company_name,
    count(DISTINCT dag.store_app) AS app_count,
    sum(dc.d30_installs) AS installs_d30,
    sum(dc.d30_rating_count) AS rating_count_d30,
    sum(dag.installs) AS installs_total,
    sum(dag.rating_count) AS rating_count_total
   FROM (distinct_apps_group dag
     LEFT JOIN d30_counts dc ON ((dag.store_app = dc.store_app)))
  GROUP BY dag.store, dag.app_category, dag.company_domain, dag.company_name
  WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.companies_parent_category_stats OWNER TO postgres;

--
-- Name: companies_parent_category_tag_stats; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.companies_parent_category_tag_stats AS
 WITH d30_counts AS (
         SELECT sahw.store_app,
            sum(sahw.installs_diff) AS d30_installs,
            sum(sahw.rating_count_diff) AS d30_rating_count
           FROM public.app_global_metrics_weekly_diffs sahw
          WHERE ((sahw.week_start > (CURRENT_DATE - '31 days'::interval)) AND ((sahw.installs_diff > (0)::numeric) OR (sahw.rating_count_diff > (0)::numeric)))
          GROUP BY sahw.store_app
        ), distinct_apps_group AS (
         SELECT sa.store,
            csac.store_app,
            csac.app_category,
            tag.tag_source,
            c.name AS company_name,
            sa.installs,
            sa.rating_count,
            COALESCE(ad.domain_name, csac.ad_domain) AS company_domain
           FROM ((((adtech.combined_store_apps_companies csac
             LEFT JOIN adtech.companies c ON ((csac.parent_id = c.id)))
             LEFT JOIN public.domains ad ON ((c.domain_id = ad.id)))
             LEFT JOIN frontend.store_apps_overview sa ON ((csac.store_app = sa.id)))
             CROSS JOIN LATERAL ( VALUES ('sdk'::text,csac.sdk), ('api_call'::text,csac.api_call), ('app_ads_direct'::text,csac.app_ads_direct), ('app_ads_reseller'::text,csac.app_ads_reseller)) tag(tag_source, present))
          WHERE ((tag.present IS TRUE) AND (csac.parent_id IN ( SELECT DISTINCT pc.id
                   FROM (adtech.companies pc
                     LEFT JOIN adtech.companies c_1 ON ((pc.id = c_1.parent_company_id)))
                  WHERE (c_1.id IS NOT NULL))))
        )
 SELECT dag.store,
    dag.app_category,
    dag.tag_source,
    dag.company_domain,
    dag.company_name,
    count(DISTINCT dag.store_app) AS app_count,
    sum(dc.d30_installs) AS installs_d30,
    sum(dc.d30_rating_count) AS rating_count_d30,
    sum(dag.installs) AS installs_total,
    sum(dag.rating_count) AS rating_count_total
   FROM (distinct_apps_group dag
     LEFT JOIN d30_counts dc ON ((dag.store_app = dc.store_app)))
  GROUP BY dag.store, dag.app_category, dag.tag_source, dag.company_domain, dag.company_name
  WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.companies_parent_category_tag_stats OWNER TO postgres;

--
-- Name: companies_sdks_overview; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.companies_sdks_overview AS
 SELECT c.name AS company_name,
    ad.domain_name AS company_domain,
    parad.domain_name AS parent_company_domain,
    sdk.sdk_name,
    sp.package_pattern,
    sp2.path_pattern,
    COALESCE(cc.name, c.name) AS parent_company_name
   FROM ((((((adtech.companies c
     LEFT JOIN adtech.companies cc ON ((c.parent_company_id = cc.id)))
     LEFT JOIN public.domains ad ON ((c.domain_id = ad.id)))
     LEFT JOIN public.domains parad ON ((cc.domain_id = parad.id)))
     LEFT JOIN adtech.sdks sdk ON ((c.id = sdk.company_id)))
     LEFT JOIN adtech.sdk_packages sp ON ((sdk.id = sp.sdk_id)))
     LEFT JOIN adtech.sdk_paths sp2 ON ((sdk.id = sp2.sdk_id)))
  WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.companies_sdks_overview OWNER TO postgres;

--
-- Name: company_domain_country; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.company_domain_country AS
 WITH country_totals AS (
         SELECT api_call_countries.company_domain,
            api_call_countries.country,
            sum(api_call_countries.store_app_count) AS total_app_count
           FROM frontend.api_call_countries
          GROUP BY api_call_countries.company_domain, api_call_countries.country
        ), parent_country_totals AS (
         SELECT api_call_countries.parent_company_domain,
            api_call_countries.country,
            sum(api_call_countries.store_app_count) AS total_app_count
           FROM frontend.api_call_countries
          GROUP BY api_call_countries.parent_company_domain, api_call_countries.country
        ), parent_ranked_countries AS (
         SELECT parent_country_totals.parent_company_domain,
            parent_country_totals.country,
            parent_country_totals.total_app_count,
            row_number() OVER (PARTITION BY parent_country_totals.parent_company_domain ORDER BY parent_country_totals.total_app_count DESC) AS rn
           FROM parent_country_totals
        ), company_ranked_countries AS (
         SELECT country_totals.company_domain,
            country_totals.country,
            country_totals.total_app_count,
            row_number() OVER (PARTITION BY country_totals.company_domain ORDER BY country_totals.total_app_count DESC) AS rn
           FROM country_totals
        )
 SELECT company_ranked_countries.company_domain,
    company_ranked_countries.country AS most_common_country,
    company_ranked_countries.total_app_count
   FROM company_ranked_countries
  WHERE ((NOT ((company_ranked_countries.company_domain)::text IN ( SELECT parent_ranked_countries.parent_company_domain
           FROM parent_ranked_countries))) AND (company_ranked_countries.rn = 1))
UNION
 SELECT parent_ranked_countries.parent_company_domain AS company_domain,
    parent_ranked_countries.country AS most_common_country,
    parent_ranked_countries.total_app_count
   FROM parent_ranked_countries
  WHERE (parent_ranked_countries.rn = 1)
  WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.company_domain_country OWNER TO postgres;

--
-- Name: company_domains_top_apps; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.company_domains_top_apps AS
 WITH deduped_data AS (
         SELECT DISTINCT saac.tld_url AS company_domain,
            c_1.name AS company_name,
            sa.store,
            sa.name,
            sa.store_id,
            cm.mapped_category AS app_category,
            sa.installs_sum_4w AS installs_d30,
            sa.ratings_sum_4w AS rating_count_d30,
            false AS sdk,
            true AS api_call,
            false AS app_ads_direct
           FROM ((((((public.api_calls saac
             LEFT JOIN public.store_apps sa_1 ON ((saac.store_app = sa_1.id)))
             LEFT JOIN public.category_mapping cm ON (((sa_1.category)::text = (cm.original_category)::text)))
             LEFT JOIN public.domains ad_1 ON ((saac.tld_url = (ad_1.domain_name)::text)))
             LEFT JOIN adtech.company_domain_mapping cdm ON ((ad_1.id = cdm.domain_id)))
             LEFT JOIN adtech.companies c_1 ON ((cdm.company_id = c_1.id)))
             LEFT JOIN frontend.store_apps_overview sa ON ((saac.store_app = sa.id)))
        ), ranked_apps AS (
         SELECT deduped_data.company_domain,
            deduped_data.company_name,
            deduped_data.store,
            deduped_data.name,
            deduped_data.store_id,
            deduped_data.app_category,
            deduped_data.installs_d30,
            deduped_data.rating_count_d30,
            deduped_data.sdk,
            deduped_data.api_call,
            deduped_data.app_ads_direct,
            row_number() OVER (PARTITION BY deduped_data.store, deduped_data.company_domain, deduped_data.company_name ORDER BY GREATEST((COALESCE(deduped_data.rating_count_d30, (0)::numeric))::double precision, COALESCE((deduped_data.installs_d30)::double precision, (0)::double precision)) DESC) AS app_company_rank,
            row_number() OVER (PARTITION BY deduped_data.store, deduped_data.app_category, deduped_data.company_domain, deduped_data.company_name ORDER BY GREATEST((COALESCE(deduped_data.rating_count_d30, (0)::numeric))::double precision, COALESCE((deduped_data.installs_d30)::double precision, (0)::double precision)) DESC) AS app_company_category_rank
           FROM deduped_data
        )
 SELECT company_domain,
    company_name,
    store,
    name,
    store_id,
    app_category,
    installs_d30,
    rating_count_d30,
    sdk,
    api_call,
    app_ads_direct,
    app_company_rank,
    app_company_category_rank
   FROM ranked_apps
  WHERE (app_company_category_rank <= 100)
  WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.company_domains_top_apps OWNER TO postgres;

--
-- Name: company_parent_top_apps; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.company_parent_top_apps AS
 WITH deduped_data AS (
         SELECT csapc.ad_domain AS company_domain,
            c.name AS company_name,
            sa.store,
            sa.name,
            sa.store_id,
            csapc.app_category,
            sa.developer_name,
            sa.icon_url_100,
            sa.installs_sum_4w AS installs_d30,
            sa.ratings_sum_4w AS rating_count_d30,
            csapc.sdk,
            csapc.api_call,
            csapc.app_ads_direct
           FROM ((adtech.combined_store_apps_parent_companies csapc
             LEFT JOIN frontend.store_apps_overview sa ON ((csapc.store_app = sa.id)))
             LEFT JOIN adtech.companies c ON ((csapc.company_id = c.id)))
          WHERE (csapc.sdk OR csapc.api_call OR csapc.app_ads_direct)
        ), ranked_apps AS (
         SELECT deduped_data.company_domain,
            deduped_data.company_name,
            deduped_data.store,
            deduped_data.name,
            deduped_data.store_id,
            deduped_data.developer_name,
            deduped_data.icon_url_100,
            deduped_data.app_category,
            deduped_data.installs_d30,
            deduped_data.rating_count_d30,
            deduped_data.sdk,
            deduped_data.api_call,
            deduped_data.app_ads_direct,
            row_number() OVER (PARTITION BY deduped_data.store, deduped_data.company_domain, deduped_data.company_name ORDER BY (COALESCE((deduped_data.sdk)::integer, 0) + COALESCE((deduped_data.api_call)::integer, 0)) DESC, GREATEST((COALESCE(deduped_data.rating_count_d30, (0)::numeric))::double precision, COALESCE((deduped_data.installs_d30)::double precision, (0)::double precision)) DESC) AS app_company_rank,
            row_number() OVER (PARTITION BY deduped_data.store, deduped_data.app_category, deduped_data.company_domain, deduped_data.company_name ORDER BY (COALESCE((deduped_data.sdk)::integer, 0) + COALESCE((deduped_data.api_call)::integer, 0)) DESC, GREATEST((COALESCE(deduped_data.rating_count_d30, (0)::numeric))::double precision, COALESCE((deduped_data.installs_d30)::double precision, (0)::double precision)) DESC) AS app_company_category_rank
           FROM deduped_data
        )
 SELECT company_domain,
    company_name,
    store,
    name,
    store_id,
    developer_name,
    app_category,
    icon_url_100,
    installs_d30,
    rating_count_d30,
    sdk,
    api_call,
    app_ads_direct,
    app_company_rank,
    app_company_category_rank
   FROM ranked_apps
  WHERE (app_company_category_rank <= 20)
  WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.company_parent_top_apps OWNER TO postgres;

--
-- Name: company_top_apps; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.company_top_apps AS
 WITH deduped_data AS (
         SELECT cac.ad_domain AS company_domain,
            c.name AS company_name,
            sa.store,
            sa.name,
            sa.store_id,
            cac.app_category,
            sa.developer_name,
            sa.icon_url_100,
            sa.installs_sum_4w AS installs_d30,
            sa.ratings_sum_4w AS rating_count_d30,
            cac.sdk,
            cac.api_call,
            cac.app_ads_direct
           FROM ((adtech.combined_store_apps_companies cac
             LEFT JOIN frontend.store_apps_overview sa ON ((cac.store_app = sa.id)))
             LEFT JOIN adtech.companies c ON ((cac.company_id = c.id)))
          WHERE (cac.app_ads_direct OR cac.sdk OR cac.api_call)
        ), ranked_apps AS (
         SELECT deduped_data.company_domain,
            deduped_data.company_name,
            deduped_data.store,
            deduped_data.name,
            deduped_data.store_id,
            deduped_data.developer_name,
            deduped_data.icon_url_100,
            deduped_data.app_category,
            deduped_data.installs_d30,
            deduped_data.rating_count_d30,
            deduped_data.sdk,
            deduped_data.api_call,
            deduped_data.app_ads_direct,
            row_number() OVER (PARTITION BY deduped_data.store, deduped_data.company_domain, deduped_data.company_name ORDER BY (COALESCE((deduped_data.sdk)::integer, 0) + COALESCE((deduped_data.api_call)::integer, 0)) DESC, GREATEST((COALESCE(deduped_data.rating_count_d30, (0)::numeric))::double precision, COALESCE((deduped_data.installs_d30)::double precision, (0)::double precision)) DESC) AS app_company_rank,
            row_number() OVER (PARTITION BY deduped_data.store, deduped_data.app_category, deduped_data.company_domain, deduped_data.company_name ORDER BY (COALESCE((deduped_data.sdk)::integer, 0) + COALESCE((deduped_data.api_call)::integer, 0)) DESC, GREATEST((COALESCE(deduped_data.rating_count_d30, (0)::numeric))::double precision, COALESCE((deduped_data.installs_d30)::double precision, (0)::double precision)) DESC) AS app_company_category_rank
           FROM deduped_data
        )
 SELECT company_domain,
    company_name,
    store,
    name,
    store_id,
    developer_name,
    icon_url_100,
    app_category,
    installs_d30,
    rating_count_d30,
    sdk,
    api_call,
    app_ads_direct,
    app_company_rank,
    app_company_category_rank
   FROM ranked_apps
  WHERE (app_company_category_rank <= 20)
  WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.company_top_apps OWNER TO postgres;

--
-- Name: app_keywords_extracted; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.app_keywords_extracted (
    store_app integer NOT NULL,
    keyword_id integer NOT NULL,
    description_id integer NOT NULL,
    extracted_at timestamp without time zone NOT NULL
);


ALTER TABLE public.app_keywords_extracted OWNER TO postgres;

--
-- Name: keywords; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.keywords (
    id integer NOT NULL,
    keyword_text character varying(255) NOT NULL
);


ALTER TABLE public.keywords OWNER TO postgres;

--
-- Name: keyword_scores; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.keyword_scores AS
 WITH keyword_app_counts AS (
         SELECT sa.store,
            k.keyword_text,
            ake.keyword_id,
            count(DISTINCT ake.store_app) AS app_count,
            array_length(string_to_array((k.keyword_text)::text, ' '::text), 1) AS word_count
           FROM ((public.app_keywords_extracted ake
             LEFT JOIN public.keywords k ON ((ake.keyword_id = k.id)))
             LEFT JOIN public.store_apps sa ON ((ake.store_app = sa.id)))
          GROUP BY sa.store, k.keyword_text, ake.keyword_id
        ), total_app_count AS (
         SELECT sa.store,
            count(DISTINCT ake.store_app) AS total_apps
           FROM (public.app_keywords_extracted ake
             LEFT JOIN public.store_apps sa ON ((ake.store_app = sa.id)))
          GROUP BY sa.store
        ), keyword_competitors AS (
         SELECT ake.keyword_id,
            sa.store,
            avg(COALESCE(NULLIF(agml.installs, 0), (agml.rating_count * 25))) AS avg_installs,
            max(COALESCE(NULLIF(agml.installs, 0), (agml.rating_count * 25))) AS max_installs,
            percentile_cont((0.5)::double precision) WITHIN GROUP (ORDER BY ((COALESCE(NULLIF(agml.installs, 0), (agml.rating_count * 25)))::double precision)) AS median_installs,
            avg(agml.rating) AS avg_rating,
            count(*) FILTER (WHERE (COALESCE(NULLIF(agml.installs, 0), (agml.rating_count * 25)) > 1000000)) AS apps_over_1m_installs,
            count(*) FILTER (WHERE ((sa.name)::text ~~* (('%'::text || (k.keyword_text)::text) || '%'::text))) AS title_matches
           FROM (((public.app_keywords_extracted ake
             LEFT JOIN public.store_apps sa ON ((ake.store_app = sa.id)))
             LEFT JOIN public.app_global_metrics_latest agml ON ((sa.id = agml.store_app)))
             LEFT JOIN public.keywords k ON ((ake.keyword_id = k.id)))
          GROUP BY ake.keyword_id, sa.store, k.keyword_text
        ), keyword_metrics AS (
         SELECT kac.store,
            kac.keyword_text,
            kac.keyword_id,
            kac.app_count,
            round(kc.avg_installs, 0) AS avg_installs,
            tac.total_apps,
            round(((100.0 * (kac.app_count)::numeric) / (NULLIF(tac.total_apps, 0))::numeric), 2) AS market_penetration_pct,
            round(((100)::numeric * (((1)::double precision - (ln(((tac.total_apps)::double precision / ((kac.app_count + 1))::double precision)) / ln((tac.total_apps)::double precision))))::numeric), 2) AS competitiveness_score,
            kac.word_count,
                CASE
                    WHEN (kac.word_count = 1) THEN 'short_tail'::text
                    WHEN (kac.word_count = 2) THEN 'medium_tail'::text
                    ELSE 'long_tail'::text
                END AS keyword_type,
            length((kac.keyword_text)::text) AS char_length,
            (COALESCE(kc.avg_installs, (0)::numeric))::bigint AS avg_competitor_installs,
            COALESCE(kc.max_installs, (0)::bigint) AS top_competitor_installs,
            (COALESCE(kc.median_installs, (0)::double precision))::bigint AS median_competitor_installs,
            COALESCE(kc.avg_rating, (0)::double precision) AS avg_competitor_rating,
            COALESCE(kc.apps_over_1m_installs, (0)::bigint) AS major_competitors,
            COALESCE(kc.title_matches, (0)::bigint) AS title_matches,
            round(((100.0 * (COALESCE(kc.title_matches, (0)::bigint))::numeric) / (NULLIF(kac.app_count, 0))::numeric), 2) AS title_relevance_pct
           FROM ((keyword_app_counts kac
             LEFT JOIN total_app_count tac ON ((kac.store = tac.store)))
             LEFT JOIN keyword_competitors kc ON (((kac.keyword_id = kc.keyword_id) AND (kac.store = kc.store))))
        )
 SELECT store,
    keyword_text,
    keyword_id,
    app_count,
    avg_installs,
    total_apps,
    market_penetration_pct,
    competitiveness_score,
    word_count,
    keyword_type,
    char_length,
    avg_competitor_installs,
    top_competitor_installs,
    median_competitor_installs,
    avg_competitor_rating,
    major_competitors,
    title_matches,
    title_relevance_pct,
    round(LEAST((100)::numeric, ((((app_count)::numeric * 10.0) * ((100)::numeric - competitiveness_score)) / 100.0)), 2) AS volume_competition_score,
    round(LEAST((100)::numeric, ((competitiveness_score * 0.6) + (LEAST((100)::numeric, ((COALESCE(avg_competitor_installs, (0)::bigint))::numeric / 100000.0)) * 0.4))), 2) AS keyword_difficulty,
    round(
        CASE
            WHEN (app_count < 10) THEN (0)::double precision
            WHEN ((major_competitors)::numeric > ((app_count)::numeric * 0.25)) THEN (20)::double precision
            ELSE ((LEAST((40)::double precision, (log(((app_count + 1))::double precision) * (10)::double precision)) + ((((100)::numeric - competitiveness_score) * 0.4))::double precision) + (
            CASE
                WHEN (COALESCE(median_competitor_installs, (0)::bigint) < 100000) THEN 20
                WHEN (COALESCE(median_competitor_installs, (0)::bigint) < 1000000) THEN 15
                WHEN (COALESCE(median_competitor_installs, (0)::bigint) < 10000000) THEN 10
                ELSE 5
            END)::double precision)
        END) AS opportunity_score,
        CASE
            WHEN (app_count > 0) THEN round(((((app_count)::numeric * 1000.0) * (1.0 / ((1)::numeric + (competitiveness_score / 50.0)))) *
            CASE
                WHEN (word_count = 1) THEN 2.0
                WHEN (word_count = 2) THEN 1.0
                ELSE 0.5
            END), 0)
            ELSE (0)::numeric
        END AS estimated_monthly_searches,
    round(((100)::numeric - LEAST((100)::numeric, ((((major_competitors)::numeric * 10.0) + ((COALESCE(median_competitor_installs, (0)::bigint))::numeric / 100000.0)) + (competitiveness_score * 0.3)))), 2) AS ranking_feasibility
   FROM keyword_metrics km
  WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.keyword_scores OWNER TO postgres;

--
-- Name: latest_sdk_scanned_apps; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.latest_sdk_scanned_apps AS
 WITH latest_version_codes AS (
         SELECT DISTINCT ON (version_codes.store_app) version_codes.id,
            version_codes.store_app,
            version_codes.version_code,
            version_codes.updated_at,
            version_codes.crawl_result
           FROM public.version_codes
          ORDER BY version_codes.store_app, (string_to_array((version_codes.version_code)::text, '.'::text))::bigint[] DESC
        ), ranked_apps AS (
         SELECT lvc.updated_at AS sdk_crawled_at,
            lvc.version_code,
            lvc.crawl_result,
            sa.store,
            sa.store_id,
            sa.name,
            sa.installs,
            sa.rating_count,
            row_number() OVER (PARTITION BY sa.store, lvc.crawl_result ORDER BY lvc.updated_at DESC) AS updated_rank
           FROM (latest_version_codes lvc
             LEFT JOIN frontend.store_apps_overview sa ON ((lvc.store_app = sa.id)))
          WHERE (lvc.updated_at <= (CURRENT_DATE - '1 day'::interval))
        )
 SELECT sdk_crawled_at,
    version_code,
    crawl_result,
    store,
    store_id,
    name,
    installs,
    rating_count,
    updated_rank
   FROM ranked_apps
  WHERE (updated_rank <= 100)
  WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.latest_sdk_scanned_apps OWNER TO postgres;

--
-- Name: mediation_adapter_app_counts; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.mediation_adapter_app_counts AS
 WITH filter_mediation_strings AS (
         SELECT vs.id AS string_id,
            sd.company_id AS mediation_company_id,
            vs.value_name AS full_sdk,
            regexp_replace(regexp_replace(vs.value_name, concat(cmp.mediation_pattern, '.'), ''::text), '\..*$'::text, ''::text) AS adapter_string
           FROM ((public.version_strings vs
             JOIN adtech.sdk_mediation_patterns cmp ON ((lower(vs.value_name) ~~ (lower(concat((cmp.mediation_pattern)::text, '.')) || '%'::text))))
             JOIN adtech.sdks sd ON ((cmp.sdk_id = sd.id)))
        ), mediation_strings AS (
         SELECT fms.string_id,
            fms.mediation_company_id,
            cma.company_id AS adapter_company_id,
            fms.adapter_string,
            fms.full_sdk
           FROM (filter_mediation_strings fms
             LEFT JOIN adtech.company_mediation_adapters cma ON ((lower(fms.adapter_string) ~~ (lower((cma.adapter_pattern)::text) || '%'::text))))
          WHERE (fms.mediation_company_id <> cma.company_id)
        ), app_counts AS (
         SELECT ms.mediation_company_id,
            ms.adapter_string,
            ms.adapter_company_id,
            cm.mapped_category AS app_category,
            count(DISTINCT sass.store_app) AS app_count
           FROM (((adtech.store_app_sdk_strings sass
             JOIN mediation_strings ms ON ((sass.version_string_id = ms.string_id)))
             LEFT JOIN public.store_apps sa ON ((sass.store_app = sa.id)))
             LEFT JOIN public.category_mapping cm ON (((sa.category)::text = (cm.original_category)::text)))
          GROUP BY ms.mediation_company_id, ms.adapter_string, ms.adapter_company_id, cm.mapped_category
        )
 SELECT md.domain_name AS mediation_domain,
    ac.adapter_string,
    ad.domain_name AS adapter_domain,
    adc.name AS adapter_company_name,
    adc.logo_url AS adapter_logo_url,
    ac.app_category,
    ac.app_count
   FROM ((((app_counts ac
     LEFT JOIN adtech.companies mdc ON ((ac.mediation_company_id = mdc.id)))
     LEFT JOIN public.domains md ON ((mdc.domain_id = md.id)))
     LEFT JOIN adtech.companies adc ON ((ac.adapter_company_id = adc.id)))
     LEFT JOIN public.domains ad ON ((adc.domain_id = ad.id)))
  WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.mediation_adapter_app_counts OWNER TO postgres;

--
-- Name: store_app_api_companies; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.store_app_api_companies AS
 WITH latest_run_per_app AS (
         SELECT DISTINCT ON (saac_1.store_app) saac_1.store_app,
            saac_1.run_id
           FROM (public.api_calls saac_1
             JOIN public.version_code_api_scan_results vcasr ON ((saac_1.run_id = vcasr.id)))
          ORDER BY saac_1.store_app, vcasr.run_at DESC
        )
 SELECT DISTINCT sa.store_id,
    ac.tld_url AS company_domain,
    c.id AS company_id,
    c.name AS company_name,
    co.alpha2 AS country
   FROM ((((((((((latest_run_per_app lrpa
     LEFT JOIN public.api_calls ac ON ((lrpa.run_id = ac.run_id)))
     LEFT JOIN public.domains ad ON ((ac.tld_url = (ad.domain_name)::text)))
     LEFT JOIN public.store_apps sa ON ((ac.store_app = sa.id)))
     LEFT JOIN adtech.company_domain_mapping cdm ON ((ad.id = cdm.domain_id)))
     LEFT JOIN adtech.companies c ON ((cdm.company_id = c.id)))
     LEFT JOIN public.domains cad ON ((c.domain_id = cad.id)))
     LEFT JOIN adtech.companies pc ON ((c.parent_company_id = pc.id)))
     LEFT JOIN public.domains pcad ON ((pc.domain_id = pcad.id)))
     LEFT JOIN public.ip_geo_snapshots igs ON ((ac.ip_geo_snapshot_id = igs.id)))
     LEFT JOIN public.countries co ON ((igs.country_id = co.id)))
  ORDER BY sa.store_id DESC
  WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.store_app_api_companies OWNER TO postgres;

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
-- Name: store_collections; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.store_collections (
    id smallint NOT NULL,
    store smallint NOT NULL,
    collection character varying NOT NULL
);


ALTER TABLE public.store_collections OWNER TO postgres;

--
-- Name: store_app_ranks_best_monthly; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.store_app_ranks_best_monthly AS
 WITH overview_ranks AS (
         SELECT sa.store_id,
            c.alpha2 AS country,
            sc.collection,
            sca.category,
            min(sarw.best_rank) AS best_rank
           FROM ((((frontend.store_app_ranks_weekly sarw
             LEFT JOIN public.store_apps sa ON ((sarw.store_app = sa.id)))
             LEFT JOIN public.store_collections sc ON ((sarw.store_collection = sc.id)))
             LEFT JOIN public.store_categories sca ON ((sarw.store_category = sca.id)))
             LEFT JOIN public.countries c ON ((sarw.country = c.id)))
          WHERE (sarw.crawled_date >= (CURRENT_DATE - '30 days'::interval))
          GROUP BY sa.store_id, c.alpha2, sc.collection, sca.category
        )
 SELECT store_id,
    country,
    collection,
    category,
    best_rank
   FROM overview_ranks
  WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.store_app_ranks_best_monthly OWNER TO postgres;

--
-- Name: store_app_ranks_daily; Type: TABLE; Schema: frontend; Owner: postgres
--

CREATE TABLE frontend.store_app_ranks_daily (
    rank smallint NOT NULL,
    best_rank smallint NOT NULL,
    country smallint NOT NULL,
    store_collection smallint NOT NULL,
    store_category smallint NOT NULL,
    crawled_date date NOT NULL,
    store_app integer NOT NULL
);


ALTER TABLE frontend.store_app_ranks_daily OWNER TO postgres;

--
-- Name: store_app_ranks_latest; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.store_app_ranks_latest AS
 WITH latest_crawled_date AS (
         SELECT arr.store_collection,
            arr.country,
            max(arr.crawled_date) AS crawled_date
           FROM frontend.store_app_ranks_weekly arr
          GROUP BY arr.store_collection, arr.country
        )
 SELECT ar.rank,
    sa.name,
    sa.store_id,
    sa.store,
    sa.developer_name,
    sa.installs,
    sa.rating_count,
    sa.rating,
    sa.installs_sum_1w,
    sa.installs_sum_4w,
    sa.ratings_sum_1w,
    sa.ratings_sum_4w,
    sa.icon_url_100,
    ar.store_collection,
    ar.store_category,
    c.alpha2 AS country,
    ar.crawled_date
   FROM (((frontend.store_app_ranks_weekly ar
     JOIN latest_crawled_date lcd ON (((ar.store_collection = lcd.store_collection) AND (ar.country = lcd.country) AND (ar.crawled_date = lcd.crawled_date))))
     JOIN public.countries c ON ((ar.country = c.id)))
     LEFT JOIN frontend.store_apps_overview sa ON ((ar.store_app = sa.id)))
  ORDER BY ar.rank
  WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.store_app_ranks_latest OWNER TO postgres;

--
-- Name: store_apps_z_scores; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.store_apps_z_scores AS
 WITH app_metrics AS (
         SELECT app_global_metrics_latest.store_app,
            app_global_metrics_latest.rating,
            app_global_metrics_latest.rating_count,
            app_global_metrics_latest.installs
           FROM public.app_global_metrics_latest
        ), ranked_z_scores AS (
         SELECT saz.store_app,
            saz.latest_week,
            saz.installs_sum_1w,
            saz.ratings_sum_1w,
            saz.installs_avg_2w,
            saz.ratings_avg_2w,
            saz.installs_z_score_2w,
            saz.ratings_z_score_2w,
            saz.installs_sum_4w,
            saz.ratings_sum_4w,
            saz.installs_avg_4w,
            saz.ratings_avg_4w,
            saz.installs_z_score_4w,
            saz.ratings_z_score_4w,
            sa.id,
            sa.developer,
            sa.name,
            sa.store_id,
            sa.store,
            sa.category,
            am.installs,
            sa.free,
            sa.price,
            sa.store_last_updated,
            sa.content_rating,
            sa.ad_supported,
            sa.in_app_purchases,
            sa.created_at,
            sa.updated_at,
            sa.crawl_result,
            sa.release_date,
            am.rating_count,
            sa.icon_url_100,
            cm.original_category,
            cm.mapped_category,
            row_number() OVER (PARTITION BY sa.store, cm.mapped_category,
                CASE
                    WHEN (sa.store = 2) THEN 'rating'::text
                    ELSE 'installs'::text
                END ORDER BY
                CASE
                    WHEN (sa.store = 2) THEN saz.ratings_z_score_2w
                    WHEN (sa.store = 1) THEN saz.installs_z_score_2w
                    ELSE NULL::numeric
                END DESC NULLS LAST) AS rn
           FROM (((public.store_app_z_scores saz
             LEFT JOIN public.store_apps sa ON ((saz.store_app = sa.id)))
             LEFT JOIN app_metrics am ON ((saz.store_app = am.store_app)))
             LEFT JOIN public.category_mapping cm ON (((sa.category)::text = (cm.original_category)::text)))
          WHERE (sa.store = ANY (ARRAY[1, 2]))
        )
 SELECT store,
    store_id,
    name AS app_name,
    mapped_category AS app_category,
    in_app_purchases,
    ad_supported,
    icon_url_100,
    installs,
    rating_count,
    installs_sum_1w,
    ratings_sum_1w,
    installs_avg_2w,
    ratings_avg_2w,
    installs_z_score_2w,
    ratings_z_score_2w,
    installs_sum_4w,
    ratings_sum_4w,
    installs_avg_4w,
    ratings_avg_4w,
    installs_z_score_4w,
    ratings_z_score_4w
   FROM ranked_z_scores
  WHERE (rn <= 100)
  WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.store_apps_z_scores OWNER TO postgres;

--
-- Name: total_categories_app_counts; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.total_categories_app_counts AS
 SELECT sa.store,
    tag.tag_source,
    csac.app_category,
    count(DISTINCT csac.store_app) AS app_count
   FROM ((adtech.combined_store_apps_companies csac
     LEFT JOIN public.store_apps sa ON ((csac.store_app = sa.id)))
     CROSS JOIN LATERAL ( VALUES ('sdk'::text,csac.sdk), ('api_call'::text,csac.api_call), ('app_ads_direct'::text,csac.app_ads_direct), ('app_ads_reseller'::text,csac.app_ads_reseller)) tag(tag_source, present))
  WHERE (tag.present IS TRUE)
  GROUP BY sa.store, tag.tag_source, csac.app_category
  WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.total_categories_app_counts OWNER TO postgres;

--
-- Name: app_country_crawls; Type: TABLE; Schema: logging; Owner: postgres
--

CREATE TABLE logging.app_country_crawls (
    crawl_result smallint,
    store_app integer,
    country_id smallint,
    crawled_at timestamp without time zone
);


ALTER TABLE logging.app_country_crawls OWNER TO postgres;

--
-- Name: app_description_keywords_extracted; Type: TABLE; Schema: logging; Owner: postgres
--

CREATE TABLE logging.app_description_keywords_extracted (
    store_app integer NOT NULL,
    description_id integer NOT NULL,
    extracted_at timestamp without time zone
);


ALTER TABLE logging.app_description_keywords_extracted OWNER TO postgres;

--
-- Name: creative_scan_results; Type: TABLE; Schema: logging; Owner: postgres
--

CREATE TABLE logging.creative_scan_results (
    url text,
    tld_url text,
    path text,
    content_type text,
    run_id text,
    pub_store_id text,
    file_extension text,
    creative_size bigint,
    error_msg text,
    inserted_at character varying DEFAULT now() NOT NULL
);


ALTER TABLE logging.creative_scan_results OWNER TO postgres;

--
-- Name: developers_crawled_at; Type: TABLE; Schema: logging; Owner: postgres
--

CREATE TABLE logging.developers_crawled_at (
    developer integer NOT NULL,
    apps_crawled_at timestamp without time zone
);


ALTER TABLE logging.developers_crawled_at OWNER TO postgres;

--
-- Name: keywords_crawled_at; Type: TABLE; Schema: logging; Owner: postgres
--

CREATE TABLE logging.keywords_crawled_at (
    keyword integer NOT NULL,
    crawled_at timestamp without time zone NOT NULL
);


ALTER TABLE logging.keywords_crawled_at OWNER TO postgres;

--
-- Name: snapshot_pub_domains; Type: TABLE; Schema: logging; Owner: postgres
--

CREATE TABLE logging.snapshot_pub_domains (
    updated_at timestamp without time zone DEFAULT timezone('utc'::text, now()) NOT NULL,
    crawl_result integer,
    total_rows integer,
    avg_days numeric,
    max_days bigint,
    rows_older_than15 integer
);


ALTER TABLE logging.snapshot_pub_domains OWNER TO postgres;

--
-- Name: store_app_downloads; Type: TABLE; Schema: logging; Owner: postgres
--

CREATE TABLE logging.store_app_downloads (
    store_app integer NOT NULL,
    version_code text NOT NULL,
    crawl_result smallint NOT NULL,
    updated_at timestamp without time zone DEFAULT timezone('utc'::text, now()) NOT NULL
);


ALTER TABLE logging.store_app_downloads OWNER TO postgres;

--
-- Name: store_app_no_creatives; Type: TABLE; Schema: logging; Owner: postgres
--

CREATE TABLE logging.store_app_no_creatives (
    store_app_id bigint,
    pub_store_id text,
    run_id text
);


ALTER TABLE logging.store_app_no_creatives OWNER TO postgres;

--
-- Name: store_app_sources; Type: TABLE; Schema: logging; Owner: postgres
--

CREATE TABLE logging.store_app_sources (
    store smallint NOT NULL,
    store_app integer NOT NULL,
    crawl_source text
);


ALTER TABLE logging.store_app_sources OWNER TO postgres;

--
-- Name: store_app_waydroid_crawled_at; Type: TABLE; Schema: logging; Owner: postgres
--

CREATE TABLE logging.store_app_waydroid_crawled_at (
    store_app integer NOT NULL,
    crawl_result smallint NOT NULL,
    crawled_at timestamp without time zone
);


ALTER TABLE logging.store_app_waydroid_crawled_at OWNER TO postgres;

--
-- Name: store_apps_snapshot; Type: TABLE; Schema: logging; Owner: postgres
--

CREATE TABLE logging.store_apps_snapshot (
    updated_at timestamp without time zone DEFAULT timezone('utc'::text, now()) NOT NULL,
    store integer,
    crawl_result integer,
    total_rows integer,
    avg_days numeric,
    max_days bigint,
    rows_older_than15 integer
);


ALTER TABLE logging.store_apps_snapshot OWNER TO postgres;

--
-- Name: version_code_api_scan_results; Type: TABLE; Schema: logging; Owner: postgres
--

CREATE TABLE logging.version_code_api_scan_results (
    store_app bigint NOT NULL,
    version_code text,
    apk_hash text,
    crawl_result smallint,
    updated_at timestamp without time zone DEFAULT timezone('utc'::text, now()) NOT NULL
);


ALTER TABLE logging.version_code_api_scan_results OWNER TO postgres;

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
         SELECT vc.id AS version_code,
            vc.store_app,
            (regexp_match(vm.manifest_string, 'applovin\.sdk\.key\"\ android:value\=\"([^"]+)"'::text))[1] AS applovin_sdk_key
           FROM (public.version_manifests vm
             LEFT JOIN public.version_codes vc ON ((vm.version_code = vc.id)))
        ), version_regex AS (
         SELECT vc.id AS version_code,
            vc.store_app,
            vs.value_name AS applovin_sdk_key
           FROM ((public.version_strings vs
             LEFT JOIN public.version_details_map vdm ON ((vs.id = vdm.string_id)))
             LEFT JOIN public.version_codes vc ON ((vdm.version_code = vc.id)))
          WHERE (((vs.xml_path ~~* '%applovin%key%'::text) OR (vs.xml_path = 'applovin_settings.sdk_key'::text)) AND (length(vs.value_name) = 86))
        )
 SELECT DISTINCT manifest_regex.store_app,
    manifest_regex.applovin_sdk_key
   FROM manifest_regex
  WHERE ((manifest_regex.applovin_sdk_key IS NOT NULL) AND (manifest_regex.applovin_sdk_key !~~ '@string%'::text))
UNION
 SELECT DISTINCT version_regex.store_app,
    version_regex.applovin_sdk_key
   FROM version_regex
  WITH NO DATA;


ALTER MATERIALIZED VIEW public.ad_network_sdk_keys OWNER TO postgres;

--
-- Name: adstxt_crawl_results_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.adstxt_crawl_results_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.adstxt_crawl_results_id_seq OWNER TO postgres;

--
-- Name: adstxt_crawl_results_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.adstxt_crawl_results_id_seq OWNED BY public.adstxt_crawl_results.id;


--
-- Name: api_calls_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.api_calls_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.api_calls_id_seq OWNER TO postgres;

--
-- Name: api_calls_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.api_calls_id_seq OWNED BY public.api_calls.id;


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
-- Name: app_country_metrics_history; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.app_country_metrics_history (
    snapshot_date date NOT NULL,
    store_app integer NOT NULL,
    country_id smallint NOT NULL,
    review_count integer,
    rating real,
    rating_count integer,
    one_star integer,
    two_star integer,
    three_star integer,
    four_star integer,
    five_star integer
);


ALTER TABLE public.app_country_metrics_history OWNER TO postgres;

--
-- Name: app_country_metrics_latest; Type: MATERIALIZED VIEW; Schema: public; Owner: postgres
--

CREATE MATERIALIZED VIEW public.app_country_metrics_latest AS
 SELECT DISTINCT ON (store_app, country_id) snapshot_date,
    store_app,
    country_id,
    review_count,
    rating,
    rating_count,
    one_star,
    two_star,
    three_star,
    four_star,
    five_star
   FROM public.app_country_metrics_history sacs
  ORDER BY store_app, country_id, snapshot_date DESC
  WITH NO DATA;


ALTER MATERIALIZED VIEW public.app_country_metrics_latest OWNER TO postgres;

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
-- Name: crawl_scenario_country_config; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.crawl_scenario_country_config (
    id integer NOT NULL,
    country_id integer,
    scenario_id integer,
    enabled boolean DEFAULT true,
    priority integer DEFAULT 1,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now()
);


ALTER TABLE public.crawl_scenario_country_config OWNER TO postgres;

--
-- Name: crawl_scenario_country_config_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.crawl_scenario_country_config_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.crawl_scenario_country_config_id_seq OWNER TO postgres;

--
-- Name: crawl_scenario_country_config_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.crawl_scenario_country_config_id_seq OWNED BY public.crawl_scenario_country_config.id;


--
-- Name: crawl_scenarios; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.crawl_scenarios (
    id integer NOT NULL,
    name character varying(50) NOT NULL,
    description text,
    created_at timestamp with time zone DEFAULT now()
);


ALTER TABLE public.crawl_scenarios OWNER TO postgres;

--
-- Name: crawl_scenarios_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.crawl_scenarios_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.crawl_scenarios_id_seq OWNER TO postgres;

--
-- Name: crawl_scenarios_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.crawl_scenarios_id_seq OWNED BY public.crawl_scenarios.id;


--
-- Name: creative_assets_new_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.creative_assets_new_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.creative_assets_new_id_seq OWNER TO postgres;

--
-- Name: creative_assets_new_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.creative_assets_new_id_seq OWNED BY public.creative_assets.id;


--
-- Name: creative_records_id_seq1; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.creative_records_id_seq1
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.creative_records_id_seq1 OWNER TO postgres;

--
-- Name: creative_records_id_seq1; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.creative_records_id_seq1 OWNED BY public.creative_records.id;


--
-- Name: developer_store_apps; Type: MATERIALIZED VIEW; Schema: public; Owner: postgres
--

CREATE MATERIALIZED VIEW public.developer_store_apps AS
 WITH developer_domain_ids AS (
         SELECT DISTINCT pd_1.id AS domain_id
           FROM (((public.app_urls_map aum_1
             LEFT JOIN public.domains pd_1 ON ((aum_1.pub_domain = pd_1.id)))
             LEFT JOIN public.store_apps sa_1 ON ((aum_1.store_app = sa_1.id)))
             LEFT JOIN public.developers d_1 ON ((sa_1.developer = d_1.id)))
        )
 SELECT sa.store,
    sa.id AS store_app,
    d.name AS developer_name,
    pd.domain_name AS developer_url,
    d.store AS developer_store,
    pd.id AS domain_id,
    d.developer_id
   FROM (((public.store_apps sa
     LEFT JOIN public.developers d ON ((sa.developer = d.id)))
     LEFT JOIN public.app_urls_map aum ON ((sa.id = aum.store_app)))
     LEFT JOIN public.domains pd ON ((aum.pub_domain = pd.id)))
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
-- Name: domains_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.domains_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.domains_id_seq OWNER TO postgres;

--
-- Name: domains_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.domains_id_seq OWNED BY public.domains.id;


--
-- Name: ip_geo_snapshots_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.ip_geo_snapshots_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.ip_geo_snapshots_id_seq OWNER TO postgres;

--
-- Name: ip_geo_snapshots_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.ip_geo_snapshots_id_seq OWNED BY public.ip_geo_snapshots.id;


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
 SELECT sa.store,
    cm.mapped_category AS category,
    count(*) AS app_count
   FROM (public.store_apps sa
     JOIN public.category_mapping cm ON (((sa.category)::text = (cm.original_category)::text)))
  WHERE ((sa.crawl_result = 1) AND (sa.category IS NOT NULL))
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
-- Name: store_app_z_scores_history; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.store_app_z_scores_history (
    target_week date NOT NULL,
    store character varying NOT NULL,
    store_app numeric NOT NULL,
    store_id character varying NOT NULL,
    app_name character varying NOT NULL,
    in_app_purchases boolean,
    ad_supported boolean,
    icon_url_100 text,
    icon_url_512 text,
    installs bigint,
    rating_count bigint,
    target_week_installs numeric,
    target_week_rating_count numeric,
    baseline_installs_2w numeric,
    baseline_ratings_2w numeric,
    installs_pct_increase numeric,
    ratings_pct_increase numeric,
    installs_z_score_1w numeric,
    ratings_z_score_1w numeric
);


ALTER TABLE public.store_app_z_scores_history OWNER TO postgres;

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
         SELECT sa.id AS store_app,
            sa.store,
            sa.store_last_updated,
            sa.name,
            sa.installs,
            sa.rating_count,
            sa.store_id
           FROM (frontend.store_apps_z_scores saz
             LEFT JOIN frontend.store_apps_overview sa ON (((saz.store_id)::text = (sa.store_id)::text)))
          WHERE sa.free
          ORDER BY COALESCE(saz.installs_z_score_2w, saz.ratings_z_score_2w) DESC
         LIMIT 500
        ), ranked_apps AS (
         SELECT DISTINCT ON (ar.store_app) ar.store_app,
            sa.store,
            sa.store_last_updated,
            sa.name,
            sa.installs,
            sa.rating_count,
            sa.store_id
           FROM ((frontend.store_app_ranks_weekly ar
             LEFT JOIN frontend.store_apps_overview sa ON ((ar.store_app = sa.id)))
             LEFT JOIN public.countries c ON ((ar.country = c.id)))
          WHERE (sa.free AND (ar.store_collection = ANY (ARRAY[1, 3, 4, 6])) AND (ar.crawled_date > (CURRENT_DATE - '15 days'::interval)) AND ((c.alpha2)::text = ANY (ARRAY[('US'::character varying)::text, ('GB'::character varying)::text, ('CA'::character varying)::text, ('AR'::character varying)::text, ('CN'::character varying)::text, ('DE'::character varying)::text, ('ID'::character varying)::text, ('IN'::character varying)::text, ('JP'::character varying)::text, ('FR'::character varying)::text, ('BR'::character varying)::text, ('MX'::character varying)::text, ('KR'::character varying)::text, ('RU'::character varying)::text])) AND (ar.rank < 150))
        )
 SELECT growth_apps.store_app,
    growth_apps.store,
    growth_apps.store_last_updated,
    growth_apps.name,
    growth_apps.installs,
    growth_apps.rating_count,
    growth_apps.store_id
   FROM growth_apps
UNION
 SELECT ranked_apps.store_app,
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
-- Name: total_count_overview; Type: MATERIALIZED VIEW; Schema: public; Owner: postgres
--

CREATE MATERIALIZED VIEW public.total_count_overview AS
 WITH app_count AS (
         SELECT count(
                CASE
                    WHEN (sa.store = 1) THEN 1
                    ELSE NULL::integer
                END) AS android_apps,
            count(
                CASE
                    WHEN (sa.store = 2) THEN 1
                    ELSE NULL::integer
                END) AS ios_apps,
            count(
                CASE
                    WHEN ((sa.store = 1) AND (sa.crawl_result = 1)) THEN 1
                    ELSE NULL::integer
                END) AS success_android_apps,
            count(
                CASE
                    WHEN ((sa.store = 2) AND (sa.crawl_result = 1)) THEN 1
                    ELSE NULL::integer
                END) AS success_ios_apps,
            count(
                CASE
                    WHEN ((sa.store = 1) AND (sa.updated_at >= (CURRENT_DATE - '7 days'::interval))) THEN 1
                    ELSE NULL::integer
                END) AS weekly_scanned_android_apps,
            count(
                CASE
                    WHEN ((sa.store = 2) AND (sa.updated_at >= (CURRENT_DATE - '7 days'::interval))) THEN 1
                    ELSE NULL::integer
                END) AS weekly_scanned_ios_apps,
            count(
                CASE
                    WHEN ((sa.store = 1) AND (sa.crawl_result = 1) AND (sa.updated_at >= (CURRENT_DATE - '7 days'::interval))) THEN 1
                    ELSE NULL::integer
                END) AS weekly_success_scanned_android_apps,
            count(
                CASE
                    WHEN ((sa.store = 2) AND (sa.crawl_result = 1) AND (sa.updated_at >= (CURRENT_DATE - '7 days'::interval))) THEN 1
                    ELSE NULL::integer
                END) AS weekly_success_scanned_ios_apps
           FROM public.store_apps sa
        ), sdk_app_count AS (
         SELECT count(DISTINCT
                CASE
                    WHEN (sa.store = 1) THEN vc.store_app
                    ELSE NULL::integer
                END) AS sdk_android_apps,
            count(DISTINCT
                CASE
                    WHEN (sa.store = 2) THEN vc.store_app
                    ELSE NULL::integer
                END) AS sdk_ios_apps,
            count(DISTINCT
                CASE
                    WHEN ((sa.store = 1) AND (vc.crawl_result = 1)) THEN vc.store_app
                    ELSE NULL::integer
                END) AS sdk_success_android_apps,
            count(DISTINCT
                CASE
                    WHEN ((sa.store = 2) AND (vc.crawl_result = 1)) THEN vc.store_app
                    ELSE NULL::integer
                END) AS sdk_success_ios_apps,
            count(DISTINCT
                CASE
                    WHEN ((sa.store = 1) AND (vc.updated_at >= (CURRENT_DATE - '7 days'::interval)) AND (vc.crawl_result = 1)) THEN vc.store_app
                    ELSE NULL::integer
                END) AS sdk_weekly_success_android_apps,
            count(DISTINCT
                CASE
                    WHEN ((sa.store = 2) AND (vc.updated_at >= (CURRENT_DATE - '7 days'::interval)) AND (vc.crawl_result = 1)) THEN vc.store_app
                    ELSE NULL::integer
                END) AS sdk_weekly_success_ios_apps,
            count(DISTINCT
                CASE
                    WHEN ((sa.store = 1) AND (vc.updated_at >= (CURRENT_DATE - '7 days'::interval))) THEN vc.store_app
                    ELSE NULL::integer
                END) AS sdk_weekly_android_apps,
            count(DISTINCT
                CASE
                    WHEN ((sa.store = 2) AND (vc.updated_at >= (CURRENT_DATE - '7 days'::interval))) THEN vc.store_app
                    ELSE NULL::integer
                END) AS sdk_weekly_ios_apps
           FROM (public.version_codes vc
             LEFT JOIN public.store_apps sa ON ((vc.store_app = sa.id)))
        ), appads_url_count AS (
         SELECT count(DISTINCT pd.domain_name) AS appads_urls,
            count(DISTINCT
                CASE
                    WHEN (pdcr.crawl_result = 1) THEN pd.domain_name
                    ELSE NULL::character varying
                END) AS appads_success_urls,
            count(DISTINCT
                CASE
                    WHEN ((pdcr.crawl_result = 1) AND (pdcr.updated_at >= (CURRENT_DATE - '7 days'::interval))) THEN pd.domain_name
                    ELSE NULL::character varying
                END) AS appads_weekly_success_urls,
            count(DISTINCT
                CASE
                    WHEN (pdcr.updated_at >= (CURRENT_DATE - '7 days'::interval)) THEN pd.domain_name
                    ELSE NULL::character varying
                END) AS appads_weekly_urls
           FROM (public.domains pd
             LEFT JOIN public.adstxt_crawl_results pdcr ON ((pd.id = pdcr.domain_id)))
        )
 SELECT app_count.android_apps,
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
    CURRENT_DATE AS on_date
   FROM app_count,
    sdk_app_count,
    appads_url_count
  WITH NO DATA;


ALTER MATERIALIZED VIEW public.total_count_overview OWNER TO postgres;

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
-- Name: api_call_urls id; Type: DEFAULT; Schema: adtech; Owner: postgres
--

ALTER TABLE ONLY adtech.api_call_urls ALTER COLUMN id SET DEFAULT nextval('adtech.api_call_urls_id_seq'::regclass);


--
-- Name: url_redirect_chains id; Type: DEFAULT; Schema: adtech; Owner: postgres
--

ALTER TABLE ONLY adtech.url_redirect_chains ALTER COLUMN id SET DEFAULT nextval('adtech.url_redirect_chains_id_seq'::regclass);


--
-- Name: urls id; Type: DEFAULT; Schema: adtech; Owner: postgres
--

ALTER TABLE ONLY adtech.urls ALTER COLUMN id SET DEFAULT nextval('adtech.urls_id_seq'::regclass);


--
-- Name: adstxt_crawl_results id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.adstxt_crawl_results ALTER COLUMN id SET DEFAULT nextval('public.adstxt_crawl_results_id_seq'::regclass);


--
-- Name: api_calls id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.api_calls ALTER COLUMN id SET DEFAULT nextval('public.api_calls_id_seq'::regclass);


--
-- Name: crawl_scenario_country_config id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.crawl_scenario_country_config ALTER COLUMN id SET DEFAULT nextval('public.crawl_scenario_country_config_id_seq'::regclass);


--
-- Name: crawl_scenarios id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.crawl_scenarios ALTER COLUMN id SET DEFAULT nextval('public.crawl_scenarios_id_seq'::regclass);


--
-- Name: creative_assets id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.creative_assets ALTER COLUMN id SET DEFAULT nextval('public.creative_assets_new_id_seq'::regclass);


--
-- Name: creative_records id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.creative_records ALTER COLUMN id SET DEFAULT nextval('public.creative_records_id_seq1'::regclass);


--
-- Name: domains id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.domains ALTER COLUMN id SET DEFAULT nextval('public.domains_id_seq'::regclass);


--
-- Name: ip_geo_snapshots id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.ip_geo_snapshots ALTER COLUMN id SET DEFAULT nextval('public.ip_geo_snapshots_id_seq'::regclass);


--
-- Name: keywords id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.keywords ALTER COLUMN id SET DEFAULT nextval('public.keywords_id_seq'::regclass);


--
-- Name: languages id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.languages ALTER COLUMN id SET DEFAULT nextval('public.languages_id_seq'::regclass);


--
-- Name: platforms id; Type: DEFAULT; Schema: public; Owner: james
--

ALTER TABLE ONLY public.platforms ALTER COLUMN id SET DEFAULT nextval('public.newtable_id_seq'::regclass);


--
-- Name: store_apps_descriptions id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.store_apps_descriptions ALTER COLUMN id SET DEFAULT nextval('public.store_apps_descriptions_id_seq'::regclass);


--
-- Name: stores id; Type: DEFAULT; Schema: public; Owner: james
--

ALTER TABLE ONLY public.stores ALTER COLUMN id SET DEFAULT nextval('public.stores_column1_seq'::regclass);


--
-- Name: version_code_api_scan_results id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.version_code_api_scan_results ALTER COLUMN id SET DEFAULT nextval('public.version_code_api_scan_results_id_seq'::regclass);


--
-- Name: version_code_sdk_scan_results id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.version_code_sdk_scan_results ALTER COLUMN id SET DEFAULT nextval('public.version_code_sdk_scan_results_id_seq'::regclass);


--
-- Name: api_call_urls api_call_urls_pkey; Type: CONSTRAINT; Schema: adtech; Owner: postgres
--

ALTER TABLE ONLY adtech.api_call_urls
    ADD CONSTRAINT api_call_urls_pkey PRIMARY KEY (id);


--
-- Name: api_call_urls api_call_urls_run_id_api_call_id_url_id_key; Type: CONSTRAINT; Schema: adtech; Owner: postgres
--

ALTER TABLE ONLY adtech.api_call_urls
    ADD CONSTRAINT api_call_urls_run_id_api_call_id_url_id_key UNIQUE (run_id, api_call_id, url_id);


--
-- Name: categories categories_pkey; Type: CONSTRAINT; Schema: adtech; Owner: postgres
--

ALTER TABLE ONLY adtech.categories
    ADD CONSTRAINT categories_pkey PRIMARY KEY (id);


--
-- Name: companies companies_domain_unique; Type: CONSTRAINT; Schema: adtech; Owner: postgres
--

ALTER TABLE ONLY adtech.companies
    ADD CONSTRAINT companies_domain_unique UNIQUE (domain_id);


--
-- Name: companies companies_pkey; Type: CONSTRAINT; Schema: adtech; Owner: postgres
--

ALTER TABLE ONLY adtech.companies
    ADD CONSTRAINT companies_pkey PRIMARY KEY (id);


--
-- Name: company_developers company_developers_pkey; Type: CONSTRAINT; Schema: adtech; Owner: postgres
--

ALTER TABLE ONLY adtech.company_developers
    ADD CONSTRAINT company_developers_pkey PRIMARY KEY (company_id, developer_id);


--
-- Name: company_domain_mapping company_domain_mapping_pkey; Type: CONSTRAINT; Schema: adtech; Owner: postgres
--

ALTER TABLE ONLY adtech.company_domain_mapping
    ADD CONSTRAINT company_domain_mapping_pkey PRIMARY KEY (company_id, domain_id);


--
-- Name: company_mediation_adapters company_mediation_adapters_pkey; Type: CONSTRAINT; Schema: adtech; Owner: postgres
--

ALTER TABLE ONLY adtech.company_mediation_adapters
    ADD CONSTRAINT company_mediation_adapters_pkey PRIMARY KEY (company_id, adapter_pattern);


--
-- Name: sdk_mediation_patterns company_mediation_patterns_pkey; Type: CONSTRAINT; Schema: adtech; Owner: postgres
--

ALTER TABLE ONLY adtech.sdk_mediation_patterns
    ADD CONSTRAINT company_mediation_patterns_pkey PRIMARY KEY (sdk_id, mediation_pattern);


--
-- Name: sdk_categories sdk_categories_pkey; Type: CONSTRAINT; Schema: adtech; Owner: postgres
--

ALTER TABLE ONLY adtech.sdk_categories
    ADD CONSTRAINT sdk_categories_pkey PRIMARY KEY (sdk_id, category_id);


--
-- Name: sdk_packages sdk_packages_pkey; Type: CONSTRAINT; Schema: adtech; Owner: postgres
--

ALTER TABLE ONLY adtech.sdk_packages
    ADD CONSTRAINT sdk_packages_pkey PRIMARY KEY (id);


--
-- Name: sdk_paths sdk_pathss_pkey; Type: CONSTRAINT; Schema: adtech; Owner: postgres
--

ALTER TABLE ONLY adtech.sdk_paths
    ADD CONSTRAINT sdk_pathss_pkey PRIMARY KEY (id);


--
-- Name: sdks sdks_company_id_sdk_name_key; Type: CONSTRAINT; Schema: adtech; Owner: postgres
--

ALTER TABLE ONLY adtech.sdks
    ADD CONSTRAINT sdks_company_id_sdk_name_key UNIQUE (company_id, sdk_name);


--
-- Name: sdks sdks_pkey; Type: CONSTRAINT; Schema: adtech; Owner: postgres
--

ALTER TABLE ONLY adtech.sdks
    ADD CONSTRAINT sdks_pkey PRIMARY KEY (id);


--
-- Name: companies unique_company_name; Type: CONSTRAINT; Schema: adtech; Owner: postgres
--

ALTER TABLE ONLY adtech.companies
    ADD CONSTRAINT unique_company_name UNIQUE (name);


--
-- Name: sdk_packages unique_package_pattern; Type: CONSTRAINT; Schema: adtech; Owner: postgres
--

ALTER TABLE ONLY adtech.sdk_packages
    ADD CONSTRAINT unique_package_pattern UNIQUE (package_pattern);


--
-- Name: sdk_paths unique_path_pattern; Type: CONSTRAINT; Schema: adtech; Owner: postgres
--

ALTER TABLE ONLY adtech.sdk_paths
    ADD CONSTRAINT unique_path_pattern UNIQUE (path_pattern);


--
-- Name: sdks unique_sdk_slug; Type: CONSTRAINT; Schema: adtech; Owner: postgres
--

ALTER TABLE ONLY adtech.sdks
    ADD CONSTRAINT unique_sdk_slug UNIQUE (sdk_slug);


--
-- Name: url_redirect_chains url_redirect_chains_pkey; Type: CONSTRAINT; Schema: adtech; Owner: postgres
--

ALTER TABLE ONLY adtech.url_redirect_chains
    ADD CONSTRAINT url_redirect_chains_pkey PRIMARY KEY (id);


--
-- Name: urls urls_pkey; Type: CONSTRAINT; Schema: adtech; Owner: postgres
--

ALTER TABLE ONLY adtech.urls
    ADD CONSTRAINT urls_pkey PRIMARY KEY (id);


--
-- Name: app_keyword_ranks_daily app_keyword_rankings_unique_test; Type: CONSTRAINT; Schema: frontend; Owner: postgres
--

ALTER TABLE ONLY frontend.app_keyword_ranks_daily
    ADD CONSTRAINT app_keyword_rankings_unique_test UNIQUE (crawled_date, store, country, keyword_id, app_rank);


--
-- Name: store_app_ranks_daily app_rankings_unique_daily; Type: CONSTRAINT; Schema: frontend; Owner: postgres
--

ALTER TABLE ONLY frontend.store_app_ranks_daily
    ADD CONSTRAINT app_rankings_unique_daily UNIQUE (crawled_date, country, store_collection, store_category, rank);


--
-- Name: store_app_ranks_weekly app_rankings_unique_test; Type: CONSTRAINT; Schema: frontend; Owner: postgres
--

ALTER TABLE ONLY frontend.store_app_ranks_weekly
    ADD CONSTRAINT app_rankings_unique_test UNIQUE (crawled_date, country, store_collection, store_category, rank);


--
-- Name: app_description_keywords_extracted app_description_keywords_extracted_pk; Type: CONSTRAINT; Schema: logging; Owner: postgres
--

ALTER TABLE ONLY logging.app_description_keywords_extracted
    ADD CONSTRAINT app_description_keywords_extracted_pk PRIMARY KEY (store_app, description_id);


--
-- Name: developers_crawled_at developers_crawled_at_pk; Type: CONSTRAINT; Schema: logging; Owner: postgres
--

ALTER TABLE ONLY logging.developers_crawled_at
    ADD CONSTRAINT developers_crawled_at_pk PRIMARY KEY (developer);


--
-- Name: keywords_crawled_at keywords_crawled_at_pk; Type: CONSTRAINT; Schema: logging; Owner: postgres
--

ALTER TABLE ONLY logging.keywords_crawled_at
    ADD CONSTRAINT keywords_crawled_at_pk PRIMARY KEY (keyword);


--
-- Name: store_app_sources store_app_sources_pk; Type: CONSTRAINT; Schema: logging; Owner: postgres
--

ALTER TABLE ONLY logging.store_app_sources
    ADD CONSTRAINT store_app_sources_pk PRIMARY KEY (store, store_app);


--
-- Name: adstxt_crawl_results adstxt_crawl_results_domain_un; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.adstxt_crawl_results
    ADD CONSTRAINT adstxt_crawl_results_domain_un UNIQUE (domain_id);


--
-- Name: adstxt_crawl_results adstxt_crawl_results_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.adstxt_crawl_results
    ADD CONSTRAINT adstxt_crawl_results_pkey PRIMARY KEY (id);


--
-- Name: api_calls api_calls_mitm_uuid_key; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.api_calls
    ADD CONSTRAINT api_calls_mitm_uuid_key UNIQUE (mitm_uuid);


--
-- Name: api_calls api_calls_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.api_calls
    ADD CONSTRAINT api_calls_pkey PRIMARY KEY (id);


--
-- Name: app_ads_entrys app_ads_entrys_pkey; Type: CONSTRAINT; Schema: public; Owner: james
--

ALTER TABLE ONLY public.app_ads_entrys
    ADD CONSTRAINT app_ads_entrys_pkey PRIMARY KEY (id);


--
-- Name: app_ads_map app_ads_map_pkey; Type: CONSTRAINT; Schema: public; Owner: james
--

ALTER TABLE ONLY public.app_ads_map
    ADD CONSTRAINT app_ads_map_pkey PRIMARY KEY (id);


--
-- Name: app_ads_map app_ads_map_un; Type: CONSTRAINT; Schema: public; Owner: james
--

ALTER TABLE ONLY public.app_ads_map
    ADD CONSTRAINT app_ads_map_un UNIQUE (pub_domain, app_ads_entry);


--
-- Name: app_ads_entrys app_ads_txt_un; Type: CONSTRAINT; Schema: public; Owner: james
--

ALTER TABLE ONLY public.app_ads_entrys
    ADD CONSTRAINT app_ads_txt_un UNIQUE (ad_domain, publisher_id, relationship);


--
-- Name: app_urls_map app_urls_map_pkey; Type: CONSTRAINT; Schema: public; Owner: james
--

ALTER TABLE ONLY public.app_urls_map
    ADD CONSTRAINT app_urls_map_pkey PRIMARY KEY (id);


--
-- Name: app_urls_map app_urls_un; Type: CONSTRAINT; Schema: public; Owner: james
--

ALTER TABLE ONLY public.app_urls_map
    ADD CONSTRAINT app_urls_un UNIQUE (store_app);


--
-- Name: countries countries_al2; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.countries
    ADD CONSTRAINT countries_al2 UNIQUE (alpha2);


--
-- Name: countries countries_al3; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.countries
    ADD CONSTRAINT countries_al3 UNIQUE (alpha3);


--
-- Name: countries countries_pk; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.countries
    ADD CONSTRAINT countries_pk PRIMARY KEY (id);


--
-- Name: crawl_results crawl_results_pkey; Type: CONSTRAINT; Schema: public; Owner: james
--

ALTER TABLE ONLY public.crawl_results
    ADD CONSTRAINT crawl_results_pkey PRIMARY KEY (id);


--
-- Name: crawl_scenario_country_config crawl_scenario_country_config_country_id_scenario_id_key; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.crawl_scenario_country_config
    ADD CONSTRAINT crawl_scenario_country_config_country_id_scenario_id_key UNIQUE (country_id, scenario_id);


--
-- Name: crawl_scenario_country_config crawl_scenario_country_config_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.crawl_scenario_country_config
    ADD CONSTRAINT crawl_scenario_country_config_pkey PRIMARY KEY (id);


--
-- Name: crawl_scenarios crawl_scenarios_name_key; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.crawl_scenarios
    ADD CONSTRAINT crawl_scenarios_name_key UNIQUE (name);


--
-- Name: crawl_scenarios crawl_scenarios_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.crawl_scenarios
    ADD CONSTRAINT crawl_scenarios_pkey PRIMARY KEY (id);


--
-- Name: creative_assets creative_assets_new_md5_hash_key; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.creative_assets
    ADD CONSTRAINT creative_assets_new_md5_hash_key UNIQUE (md5_hash);


--
-- Name: creative_assets creative_assets_new_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.creative_assets
    ADD CONSTRAINT creative_assets_new_pkey PRIMARY KEY (id);


--
-- Name: creative_records creative_records__pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.creative_records
    ADD CONSTRAINT creative_records__pkey PRIMARY KEY (id);


--
-- Name: creative_records creative_records_api_call_id_key; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.creative_records
    ADD CONSTRAINT creative_records_api_call_id_key UNIQUE (api_call_id);


--
-- Name: app_keywords_extracted description_keywords_app_id_keyword_id_key; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.app_keywords_extracted
    ADD CONSTRAINT description_keywords_app_id_keyword_id_key UNIQUE (store_app, keyword_id);


--
-- Name: developers developers_pkey; Type: CONSTRAINT; Schema: public; Owner: james
--

ALTER TABLE ONLY public.developers
    ADD CONSTRAINT developers_pkey PRIMARY KEY (id);


--
-- Name: developers developers_un; Type: CONSTRAINT; Schema: public; Owner: james
--

ALTER TABLE ONLY public.developers
    ADD CONSTRAINT developers_un UNIQUE (store, developer_id);


--
-- Name: domains domains_domain_type_un; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.domains
    ADD CONSTRAINT domains_domain_type_un UNIQUE (domain_name);


--
-- Name: domains domains_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.domains
    ADD CONSTRAINT domains_pkey PRIMARY KEY (id);


--
-- Name: ip_geo_snapshots ip_geo_snapshots_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.ip_geo_snapshots
    ADD CONSTRAINT ip_geo_snapshots_pkey PRIMARY KEY (id);


--
-- Name: ip_geo_snapshots ip_geo_unique_key; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.ip_geo_snapshots
    ADD CONSTRAINT ip_geo_unique_key UNIQUE (mitm_uuid);


--
-- Name: keywords keywords_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.keywords
    ADD CONSTRAINT keywords_pkey PRIMARY KEY (id);


--
-- Name: languages language_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.languages
    ADD CONSTRAINT language_pkey PRIMARY KEY (id);


--
-- Name: languages language_unique_key; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.languages
    ADD CONSTRAINT language_unique_key UNIQUE (language_slug);


--
-- Name: platforms platforms_pk; Type: CONSTRAINT; Schema: public; Owner: james
--

ALTER TABLE ONLY public.platforms
    ADD CONSTRAINT platforms_pk PRIMARY KEY (id);


--
-- Name: platforms platforms_un; Type: CONSTRAINT; Schema: public; Owner: james
--

ALTER TABLE ONLY public.platforms
    ADD CONSTRAINT platforms_un UNIQUE (name);


--
-- Name: store_app_z_scores_history store_app_z_scores_history_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.store_app_z_scores_history
    ADD CONSTRAINT store_app_z_scores_history_pkey PRIMARY KEY (target_week, store_id);


--
-- Name: store_apps_descriptions store_apps_descriptions_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.store_apps_descriptions
    ADD CONSTRAINT store_apps_descriptions_pkey PRIMARY KEY (id);


--
-- Name: store_apps store_apps_pkey; Type: CONSTRAINT; Schema: public; Owner: james
--

ALTER TABLE ONLY public.store_apps
    ADD CONSTRAINT store_apps_pkey PRIMARY KEY (id);


--
-- Name: store_apps store_apps_un; Type: CONSTRAINT; Schema: public; Owner: james
--

ALTER TABLE ONLY public.store_apps
    ADD CONSTRAINT store_apps_un UNIQUE (store, store_id);


--
-- Name: store_categories store_categories_pk; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.store_categories
    ADD CONSTRAINT store_categories_pk PRIMARY KEY (id);


--
-- Name: store_categories store_categories_un; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.store_categories
    ADD CONSTRAINT store_categories_un UNIQUE (store, category);


--
-- Name: store_collections store_collections_pk; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.store_collections
    ADD CONSTRAINT store_collections_pk PRIMARY KEY (id);


--
-- Name: store_collections store_collections_un; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.store_collections
    ADD CONSTRAINT store_collections_un UNIQUE (store, collection);


--
-- Name: stores stores_pk; Type: CONSTRAINT; Schema: public; Owner: james
--

ALTER TABLE ONLY public.stores
    ADD CONSTRAINT stores_pk PRIMARY KEY (id);


--
-- Name: stores stores_un; Type: CONSTRAINT; Schema: public; Owner: james
--

ALTER TABLE ONLY public.stores
    ADD CONSTRAINT stores_un UNIQUE (name);


--
-- Name: keywords unique_keyword; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.keywords
    ADD CONSTRAINT unique_keyword UNIQUE (keyword_text);


--
-- Name: version_code_api_scan_results version_code_api_scan_results_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.version_code_api_scan_results
    ADD CONSTRAINT version_code_api_scan_results_pkey PRIMARY KEY (id);


--
-- Name: version_code_sdk_scan_results version_code_sdk_scan_results_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.version_code_sdk_scan_results
    ADD CONSTRAINT version_code_sdk_scan_results_pkey PRIMARY KEY (id);


--
-- Name: version_codes version_codes_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.version_codes
    ADD CONSTRAINT version_codes_pkey PRIMARY KEY (id);


--
-- Name: version_codes version_codes_un; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.version_codes
    ADD CONSTRAINT version_codes_un UNIQUE (store_app, version_code);


--
-- Name: version_details_map version_details_map_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.version_details_map
    ADD CONSTRAINT version_details_map_pkey PRIMARY KEY (id);


--
-- Name: version_details_map version_details_map_unique; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.version_details_map
    ADD CONSTRAINT version_details_map_unique UNIQUE (version_code, string_id);


--
-- Name: version_manifests version_manifests_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.version_manifests
    ADD CONSTRAINT version_manifests_pkey PRIMARY KEY (id);


--
-- Name: version_manifests version_manifests_un; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.version_manifests
    ADD CONSTRAINT version_manifests_un UNIQUE (version_code);


--
-- Name: version_strings version_strings_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.version_strings
    ADD CONSTRAINT version_strings_pkey PRIMARY KEY (id);


--
-- Name: version_strings version_strings_unique; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.version_strings
    ADD CONSTRAINT version_strings_unique UNIQUE (xml_path, tag, value_name);


--
-- Name: combined_store_app_companies_idx; Type: INDEX; Schema: adtech; Owner: postgres
--

CREATE UNIQUE INDEX combined_store_app_companies_idx ON adtech.combined_store_apps_companies USING btree (ad_domain, store_app, app_category, company_id);


--
-- Name: idx_combined_store_apps_parent_companies_idx; Type: INDEX; Schema: adtech; Owner: postgres
--

CREATE UNIQUE INDEX idx_combined_store_apps_parent_companies_idx ON adtech.combined_store_apps_parent_companies USING btree (ad_domain, store_app, app_category, company_id);


--
-- Name: idx_found_urls_run; Type: INDEX; Schema: adtech; Owner: postgres
--

CREATE INDEX idx_found_urls_run ON adtech.api_call_urls USING btree (run_id);


--
-- Name: idx_urls_domain_id; Type: INDEX; Schema: adtech; Owner: postgres
--

CREATE INDEX idx_urls_domain_id ON adtech.urls USING btree (domain_id);


--
-- Name: idx_urls_scheme; Type: INDEX; Schema: adtech; Owner: postgres
--

CREATE INDEX idx_urls_scheme ON adtech.urls USING btree (scheme);


--
-- Name: sdk_packages_package_pattern_idx; Type: INDEX; Schema: adtech; Owner: postgres
--

CREATE INDEX sdk_packages_package_pattern_idx ON adtech.sdk_packages USING btree (package_pattern);


--
-- Name: sdk_packages_pattern_lower_idx; Type: INDEX; Schema: adtech; Owner: postgres
--

CREATE INDEX sdk_packages_pattern_lower_idx ON adtech.sdk_packages USING btree (lower((package_pattern)::text) text_pattern_ops);


--
-- Name: sdk_packages_pattern_trgm_idx; Type: INDEX; Schema: adtech; Owner: postgres
--

CREATE INDEX sdk_packages_pattern_trgm_idx ON adtech.sdk_packages USING gin (lower((package_pattern)::text) public.gin_trgm_ops);


--
-- Name: sdk_path_pattern_idx; Type: INDEX; Schema: adtech; Owner: postgres
--

CREATE INDEX sdk_path_pattern_idx ON adtech.sdk_paths USING btree (path_pattern);


--
-- Name: sdk_paths_path_pattern_trgm_idx; Type: INDEX; Schema: adtech; Owner: postgres
--

CREATE INDEX sdk_paths_path_pattern_trgm_idx ON adtech.sdk_paths USING gin (lower((path_pattern)::text) public.gin_trgm_ops);


--
-- Name: sdk_strings_version_string_id_sdk_id_idx; Type: INDEX; Schema: adtech; Owner: postgres
--

CREATE UNIQUE INDEX sdk_strings_version_string_id_sdk_id_idx ON adtech.sdk_strings USING btree (version_string_id, sdk_id);


--
-- Name: store_app_sdk_strings_idx; Type: INDEX; Schema: adtech; Owner: postgres
--

CREATE UNIQUE INDEX store_app_sdk_strings_idx ON adtech.store_app_sdk_strings USING btree (store_app, version_string_id, sdk_id);


--
-- Name: url_redirect_chains_unique_idx; Type: INDEX; Schema: adtech; Owner: postgres
--

CREATE UNIQUE INDEX url_redirect_chains_unique_idx ON adtech.url_redirect_chains USING btree (run_id, api_call_id, url_id, next_url_id);


--
-- Name: urls_url_hash_idx; Type: INDEX; Schema: adtech; Owner: postgres
--

CREATE UNIQUE INDEX urls_url_hash_idx ON adtech.urls USING btree (url_hash);


--
-- Name: adstxt_ad_domain_overview_idx; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE INDEX adstxt_ad_domain_overview_idx ON frontend.adstxt_ad_domain_overview USING btree (ad_domain_url);


--
-- Name: adstxt_ad_domain_overview_unique_idx; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE UNIQUE INDEX adstxt_ad_domain_overview_unique_idx ON frontend.adstxt_ad_domain_overview USING btree (ad_domain_url, relationship, store);


--
-- Name: adstxt_entries_store_apps_domain_pub_idx; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE INDEX adstxt_entries_store_apps_domain_pub_idx ON frontend.adstxt_entries_store_apps USING btree (ad_domain_id, app_ad_entry_id);


--
-- Name: adstxt_entries_store_apps_idx; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE INDEX adstxt_entries_store_apps_idx ON frontend.adstxt_entries_store_apps USING btree (store_app);


--
-- Name: adstxt_entries_store_apps_unique_idx; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE UNIQUE INDEX adstxt_entries_store_apps_unique_idx ON frontend.adstxt_entries_store_apps USING btree (ad_domain_id, app_ad_entry_id, store_app);


--
-- Name: adstxt_publishers_overview_ad_domain_idx; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE INDEX adstxt_publishers_overview_ad_domain_idx ON frontend.adstxt_publishers_overview USING btree (ad_domain_url);


--
-- Name: adstxt_publishers_overview_ad_domain_unique_idx; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE UNIQUE INDEX adstxt_publishers_overview_ad_domain_unique_idx ON frontend.adstxt_publishers_overview USING btree (ad_domain_url, relationship, store, publisher_id);


--
-- Name: api_call_countries_unique; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE UNIQUE INDEX api_call_countries_unique ON frontend.api_call_countries USING btree (company_domain, parent_company_domain, tld_url, country, org);


--
-- Name: app_keyword_ranks_daily_app_lookup; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE INDEX app_keyword_ranks_daily_app_lookup ON frontend.app_keyword_ranks_daily USING btree (crawled_date, store_app);


--
-- Name: app_keyword_ranks_daily_date; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE INDEX app_keyword_ranks_daily_date ON frontend.app_keyword_ranks_daily USING btree (crawled_date);


--
-- Name: app_keywords_delete_and_insert_on; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE INDEX app_keywords_delete_and_insert_on ON frontend.app_keyword_ranks_daily USING btree (crawled_date, store);


--
-- Name: companies_apps_overview_idx; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE INDEX companies_apps_overview_idx ON frontend.companies_apps_overview USING btree (store_id);


--
-- Name: companies_apps_overview_unique_idx; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE UNIQUE INDEX companies_apps_overview_unique_idx ON frontend.companies_apps_overview USING btree (store_id, company_id, category_slug);


--
-- Name: companies_category_stats_idx; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE UNIQUE INDEX companies_category_stats_idx ON frontend.companies_category_stats USING btree (store, app_category, company_domain);


--
-- Name: companies_category_stats_query_idx; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE INDEX companies_category_stats_query_idx ON frontend.companies_category_stats USING btree (company_domain);


--
-- Name: companies_category_tag_stats__query_idx; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE INDEX companies_category_tag_stats__query_idx ON frontend.companies_category_tag_stats USING btree (company_domain);


--
-- Name: companies_category_tag_stats_idx; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE UNIQUE INDEX companies_category_tag_stats_idx ON frontend.companies_category_tag_stats USING btree (store, tag_source, app_category, company_domain);


--
-- Name: companies_open_source_percent_unique; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE UNIQUE INDEX companies_open_source_percent_unique ON frontend.companies_open_source_percent USING btree (company_domain);


--
-- Name: companies_parent_category_stats_idx; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE UNIQUE INDEX companies_parent_category_stats_idx ON frontend.companies_parent_category_stats USING btree (store, company_domain, company_name, app_category);


--
-- Name: companies_parent_category_stats_query_idx; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE INDEX companies_parent_category_stats_query_idx ON frontend.companies_parent_category_stats USING btree (company_domain);


--
-- Name: companies_parent_category_tag_stats_idx; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE UNIQUE INDEX companies_parent_category_tag_stats_idx ON frontend.companies_parent_category_tag_stats USING btree (store, company_domain, company_name, app_category, tag_source);


--
-- Name: companies_parent_category_tag_stats_query_idx; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE INDEX companies_parent_category_tag_stats_query_idx ON frontend.companies_parent_category_tag_stats USING btree (company_domain);


--
-- Name: companies_sdks_overview_unique_idx; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE UNIQUE INDEX companies_sdks_overview_unique_idx ON frontend.companies_sdks_overview USING btree (company_name, company_domain, parent_company_domain, sdk_name, package_pattern, path_pattern);


--
-- Name: frontend_store_apps_z_scores_unique; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE UNIQUE INDEX frontend_store_apps_z_scores_unique ON frontend.store_apps_z_scores USING btree (store, store_id);


--
-- Name: idx_apps_fr_new_monthly; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE UNIQUE INDEX idx_apps_fr_new_monthly ON frontend.apps_new_monthly USING btree (store, app_category, store_id);


--
-- Name: idx_apps_fr_new_weekly; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE UNIQUE INDEX idx_apps_fr_new_weekly ON frontend.apps_new_weekly USING btree (store, app_category, store_id);


--
-- Name: idx_apps_fr_new_yearly; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE UNIQUE INDEX idx_apps_fr_new_yearly ON frontend.apps_new_yearly USING btree (store, app_category, store_id);


--
-- Name: idx_apps_new_monthly; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE UNIQUE INDEX idx_apps_new_monthly ON frontend.apps_new_monthly USING btree (store, app_category, store_id);


--
-- Name: idx_apps_new_weekly; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE UNIQUE INDEX idx_apps_new_weekly ON frontend.apps_new_weekly USING btree (store, app_category, store_id);


--
-- Name: idx_apps_new_weekly_f; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE UNIQUE INDEX idx_apps_new_weekly_f ON frontend.apps_new_weekly USING btree (store, app_category, store_id);


--
-- Name: idx_apps_new_yearly; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE UNIQUE INDEX idx_apps_new_yearly ON frontend.apps_new_yearly USING btree (store, app_category, store_id);


--
-- Name: idx_category_tag_stats; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE UNIQUE INDEX idx_category_tag_stats ON frontend.category_tag_stats USING btree (store, app_category, tag_source);


--
-- Name: idx_company_parent_top_apps; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE INDEX idx_company_parent_top_apps ON frontend.company_parent_top_apps USING btree (company_domain);


--
-- Name: idx_company_parent_top_apps_domain_rank; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE INDEX idx_company_parent_top_apps_domain_rank ON frontend.company_parent_top_apps USING btree (company_domain, app_company_rank);


--
-- Name: idx_company_parent_top_apps_unique; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE UNIQUE INDEX idx_company_parent_top_apps_unique ON frontend.company_parent_top_apps USING btree (company_domain, company_name, store, name, store_id, app_category);


--
-- Name: idx_company_top_apps; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE INDEX idx_company_top_apps ON frontend.company_top_apps USING btree (company_domain);


--
-- Name: idx_company_top_apps_domain_rank; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE INDEX idx_company_top_apps_domain_rank ON frontend.company_top_apps USING btree (company_domain, app_company_rank);


--
-- Name: idx_company_top_domains_apps; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE INDEX idx_company_top_domains_apps ON frontend.company_domains_top_apps USING btree (company_domain);


--
-- Name: idx_company_top_domains_apps_domain_rank; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE INDEX idx_company_top_domains_apps_domain_rank ON frontend.company_domains_top_apps USING btree (company_domain, app_company_rank);


--
-- Name: idx_query_company_domains_top_apps; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE INDEX idx_query_company_domains_top_apps ON frontend.company_domains_top_apps USING btree (company_domain, app_category, app_company_category_rank, store);


--
-- Name: idx_query_company_top_apps; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE INDEX idx_query_company_top_apps ON frontend.company_top_apps USING btree (company_domain, app_category, app_company_category_rank, store);


--
-- Name: idx_ranks_daily_filter; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE INDEX idx_ranks_daily_filter ON frontend.store_app_ranks_daily USING btree (country, store_collection, store_category, crawled_date);


--
-- Name: idx_ranks_filter; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE INDEX idx_ranks_filter ON frontend.store_app_ranks_weekly USING btree (country, store_collection, store_category, crawled_date);


--
-- Name: idx_store_app_ranks_best_monthly_store; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE INDEX idx_store_app_ranks_best_monthly_store ON frontend.store_app_ranks_best_monthly USING btree (store_id);


--
-- Name: idx_store_app_ranks_latest_filter_sort; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE UNIQUE INDEX idx_store_app_ranks_latest_filter_sort ON frontend.store_app_ranks_latest USING btree (store_collection, store_category, country, rank);


--
-- Name: idx_total_categories_app_counts; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE UNIQUE INDEX idx_total_categories_app_counts ON frontend.total_categories_app_counts USING btree (store, tag_source, app_category);


--
-- Name: idx_unique_company_domains_top_apps; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE UNIQUE INDEX idx_unique_company_domains_top_apps ON frontend.company_domains_top_apps USING btree (company_domain, company_name, store, name, store_id, app_category);


--
-- Name: idx_unique_company_top_apps; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE UNIQUE INDEX idx_unique_company_top_apps ON frontend.company_top_apps USING btree (company_domain, company_name, store, name, store_id, app_category);


--
-- Name: keyword_scores_store_keyword_id_idx; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE UNIQUE INDEX keyword_scores_store_keyword_id_idx ON frontend.keyword_scores USING btree (store, keyword_id);


--
-- Name: latest_sdk_scanned_apps_unique_index; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE UNIQUE INDEX latest_sdk_scanned_apps_unique_index ON frontend.latest_sdk_scanned_apps USING btree (version_code, crawl_result, store, store_id);


--
-- Name: sarw_crawled_store_collection_category_country_idx; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE INDEX sarw_crawled_store_collection_category_country_idx ON frontend.store_app_ranks_weekly USING btree (crawled_date, store_app, store_collection, store_category, country);


--
-- Name: store_app_ranks_best_monthly_uidx; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE UNIQUE INDEX store_app_ranks_best_monthly_uidx ON frontend.store_app_ranks_best_monthly USING btree (store_id, country, collection, category);


--
-- Name: store_apps_overview_textsearch_idx; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE INDEX store_apps_overview_textsearch_idx ON frontend.store_apps_overview USING gin (textsearchable);


--
-- Name: store_apps_overview_unique_idx; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE UNIQUE INDEX store_apps_overview_unique_idx ON frontend.store_apps_overview USING btree (store, store_id);


--
-- Name: store_apps_overview_unique_store_app_idx; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE UNIQUE INDEX store_apps_overview_unique_store_app_idx ON frontend.store_apps_overview USING btree (id);


--
-- Name: store_apps_overview_unique_store_id_idx; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE UNIQUE INDEX store_apps_overview_unique_store_id_idx ON frontend.store_apps_overview USING btree (store_id);


--
-- Name: app_description_keywords_extrac_description_id_extracted_at_idx; Type: INDEX; Schema: logging; Owner: postgres
--

CREATE INDEX app_description_keywords_extrac_description_id_extracted_at_idx ON logging.app_description_keywords_extracted USING btree (description_id, extracted_at DESC);


--
-- Name: logging_store_app_upsert_unique; Type: INDEX; Schema: logging; Owner: postgres
--

CREATE UNIQUE INDEX logging_store_app_upsert_unique ON logging.store_app_waydroid_crawled_at USING btree (store_app, crawl_result, crawled_at);


--
-- Name: ake_latest_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX ake_latest_idx ON public.app_keywords_extracted USING btree (store_app, extracted_at DESC);


--
-- Name: app_country_metrics_history_date_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX app_country_metrics_history_date_idx ON public.app_country_metrics_history USING btree (snapshot_date);


--
-- Name: app_country_metrics_history_store_app_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX app_country_metrics_history_store_app_idx ON public.app_country_metrics_history USING btree (store_app);


--
-- Name: app_country_metrics_history_unique_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE UNIQUE INDEX app_country_metrics_history_unique_idx ON public.app_country_metrics_history USING btree (store_app, country_id, snapshot_date);


--
-- Name: app_country_metrics_latest_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE UNIQUE INDEX app_country_metrics_latest_idx ON public.app_country_metrics_latest USING btree (store_app, country_id);


--
-- Name: app_global_metrics_history_date_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX app_global_metrics_history_date_idx ON public.app_global_metrics_history USING btree (snapshot_date);


--
-- Name: app_global_metrics_history_store_app_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX app_global_metrics_history_store_app_idx ON public.app_global_metrics_history USING btree (store_app);


--
-- Name: app_global_metrics_history_unique_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE UNIQUE INDEX app_global_metrics_history_unique_idx ON public.app_global_metrics_history USING btree (store_app, snapshot_date);


--
-- Name: app_global_metrics_latest_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE UNIQUE INDEX app_global_metrics_latest_idx ON public.app_global_metrics_latest USING btree (store_app);


--
-- Name: app_global_metrics_weekly_diffs_week_start_store_app_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE UNIQUE INDEX app_global_metrics_weekly_diffs_week_start_store_app_idx ON public.app_global_metrics_weekly_diffs USING btree (week_start, store_app);


--
-- Name: app_keywords_app_index; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX app_keywords_app_index ON public.app_keywords_extracted USING btree (store_app);


--
-- Name: category_mapping_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE UNIQUE INDEX category_mapping_idx ON public.category_mapping USING btree (original_category, mapped_category);


--
-- Name: dev_textsearch_generated_idx; Type: INDEX; Schema: public; Owner: james
--

CREATE INDEX dev_textsearch_generated_idx ON public.developers USING gin (textsearchable_index_col);


--
-- Name: developer_store_apps_query; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX developer_store_apps_query ON public.developer_store_apps USING btree (developer_id);


--
-- Name: developers_developer_id_idx; Type: INDEX; Schema: public; Owner: james
--

CREATE INDEX developers_developer_id_idx ON public.developers USING btree (developer_id);


--
-- Name: developers_name_idx; Type: INDEX; Schema: public; Owner: james
--

CREATE INDEX developers_name_idx ON public.developers USING gin (to_tsvector('simple'::regconfig, (name)::text));


--
-- Name: idx_api_calls_mitm_uuid; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_api_calls_mitm_uuid ON public.api_calls USING btree (mitm_uuid);


--
-- Name: idx_api_calls_store_app; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_api_calls_store_app ON public.api_calls USING btree (store_app);


--
-- Name: idx_country_crawl_config_lookup; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_country_crawl_config_lookup ON public.crawl_scenario_country_config USING btree (scenario_id, country_id) WHERE (enabled = true);


--
-- Name: idx_creative_assets_phash; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_creative_assets_phash ON public.creative_assets USING btree (phash) WHERE (phash IS NOT NULL);


--
-- Name: idx_description_tsv; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_description_tsv ON public.store_apps_descriptions USING gin (description_tsv);


--
-- Name: idx_developer_store_apps_developer_domain; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_developer_store_apps_developer_domain ON public.developer_store_apps USING btree (developer_id, domain_id);


--
-- Name: idx_developer_store_apps_domain_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_developer_store_apps_domain_id ON public.developer_store_apps USING btree (domain_id);


--
-- Name: idx_developer_store_apps_unique; Type: INDEX; Schema: public; Owner: postgres
--

CREATE UNIQUE INDEX idx_developer_store_apps_unique ON public.developer_store_apps USING btree (developer_id, store_app);


--
-- Name: idx_ip_geo_ip_created; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_ip_geo_ip_created ON public.ip_geo_snapshots USING btree (ip_address, created_at DESC);


--
-- Name: idx_ip_geo_mitm_uuid; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_ip_geo_mitm_uuid ON public.ip_geo_snapshots USING btree (mitm_uuid);


--
-- Name: store_apps_descriptions_unique_hash_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE UNIQUE INDEX store_apps_descriptions_unique_hash_idx ON public.store_apps_descriptions USING btree (store_app, language_id, md5(description), md5(description_short));


--
-- Name: store_apps_developer_idx; Type: INDEX; Schema: public; Owner: james
--

CREATE INDEX store_apps_developer_idx ON public.store_apps USING btree (developer);


--
-- Name: store_apps_name_idx; Type: INDEX; Schema: public; Owner: james
--

CREATE INDEX store_apps_name_idx ON public.store_apps USING gin (to_tsvector('simple'::regconfig, (name)::text));


--
-- Name: store_apps_store_id_idx; Type: INDEX; Schema: public; Owner: james
--

CREATE INDEX store_apps_store_id_idx ON public.store_apps USING btree (store_id);


--
-- Name: store_apps_updated_at_idx; Type: INDEX; Schema: public; Owner: james
--

CREATE INDEX store_apps_updated_at_idx ON public.store_apps USING btree (updated_at);


--
-- Name: textsearch_generated_idx; Type: INDEX; Schema: public; Owner: james
--

CREATE INDEX textsearch_generated_idx ON public.store_apps USING gin (textsearchable_index_col);


--
-- Name: version_strings_value_name_lower_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX version_strings_value_name_lower_idx ON public.version_strings USING btree (lower(value_name) text_pattern_ops);


--
-- Name: version_strings_value_name_lower_prefix_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX version_strings_value_name_lower_prefix_idx ON public.version_strings USING btree (lower(value_name) text_pattern_ops);


--
-- Name: version_strings_value_name_trgm_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX version_strings_value_name_trgm_idx ON public.version_strings USING gin (lower(value_name) public.gin_trgm_ops);


--
-- Name: version_strings_xml_path_lower_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX version_strings_xml_path_lower_idx ON public.version_strings USING btree (lower(xml_path));


--
-- Name: version_strings_xml_path_trgm_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX version_strings_xml_path_trgm_idx ON public.version_strings USING gin (lower(xml_path) public.gin_trgm_ops);


--
-- Name: app_ads_entrys app_ads_entrys_updated_at; Type: TRIGGER; Schema: public; Owner: james
--

CREATE TRIGGER app_ads_entrys_updated_at BEFORE UPDATE ON public.app_ads_entrys FOR EACH ROW EXECUTE FUNCTION public.update_modified_column();


--
-- Name: app_ads_map app_ads_map_updated_at; Type: TRIGGER; Schema: public; Owner: james
--

CREATE TRIGGER app_ads_map_updated_at BEFORE UPDATE ON public.app_ads_map FOR EACH ROW EXECUTE FUNCTION public.update_modified_column();


--
-- Name: app_urls_map app_urls_map_updated_at; Type: TRIGGER; Schema: public; Owner: james
--

CREATE TRIGGER app_urls_map_updated_at BEFORE UPDATE ON public.app_urls_map FOR EACH ROW EXECUTE FUNCTION public.update_modified_column();


--
-- Name: developers developers_updated_at; Type: TRIGGER; Schema: public; Owner: james
--

CREATE TRIGGER developers_updated_at BEFORE UPDATE ON public.developers FOR EACH ROW EXECUTE FUNCTION public.update_modified_column();


--
-- Name: store_apps store_apps_updated_at; Type: TRIGGER; Schema: public; Owner: james
--

CREATE TRIGGER store_apps_updated_at BEFORE UPDATE ON public.store_apps FOR EACH ROW EXECUTE FUNCTION public.update_modified_column();


--
-- Name: version_codes version_codes_updated_at; Type: TRIGGER; Schema: public; Owner: postgres
--

CREATE TRIGGER version_codes_updated_at BEFORE UPDATE ON public.version_codes FOR EACH ROW EXECUTE FUNCTION public.update_modified_column();


--
-- Name: api_call_urls api_call_urls_api_call_id_fkey; Type: FK CONSTRAINT; Schema: adtech; Owner: postgres
--

ALTER TABLE ONLY adtech.api_call_urls
    ADD CONSTRAINT api_call_urls_api_call_id_fkey FOREIGN KEY (api_call_id) REFERENCES public.api_calls(id);


--
-- Name: api_call_urls api_call_urls_run_id_fkey; Type: FK CONSTRAINT; Schema: adtech; Owner: postgres
--

ALTER TABLE ONLY adtech.api_call_urls
    ADD CONSTRAINT api_call_urls_run_id_fkey FOREIGN KEY (run_id) REFERENCES public.version_code_api_scan_results(id);


--
-- Name: api_call_urls api_call_urls_url_id_fkey; Type: FK CONSTRAINT; Schema: adtech; Owner: postgres
--

ALTER TABLE ONLY adtech.api_call_urls
    ADD CONSTRAINT api_call_urls_url_id_fkey FOREIGN KEY (url_id) REFERENCES adtech.urls(id);


--
-- Name: company_domain_mapping company_domain_mapping_company_id_fkey; Type: FK CONSTRAINT; Schema: adtech; Owner: postgres
--

ALTER TABLE ONLY adtech.company_domain_mapping
    ADD CONSTRAINT company_domain_mapping_company_id_fkey FOREIGN KEY (company_id) REFERENCES adtech.companies(id);


--
-- Name: company_domain_mapping company_domain_mapping_domain_id_fkey; Type: FK CONSTRAINT; Schema: adtech; Owner: postgres
--

ALTER TABLE ONLY adtech.company_domain_mapping
    ADD CONSTRAINT company_domain_mapping_domain_id_fkey FOREIGN KEY (domain_id) REFERENCES public.domains(id) ON DELETE CASCADE;


--
-- Name: company_mediation_adapters company_mediation_adapters_company_id_fkey; Type: FK CONSTRAINT; Schema: adtech; Owner: postgres
--

ALTER TABLE ONLY adtech.company_mediation_adapters
    ADD CONSTRAINT company_mediation_adapters_company_id_fkey FOREIGN KEY (company_id) REFERENCES adtech.companies(id);


--
-- Name: sdk_mediation_patterns company_mediation_patterns_company_id_fkey; Type: FK CONSTRAINT; Schema: adtech; Owner: postgres
--

ALTER TABLE ONLY adtech.sdk_mediation_patterns
    ADD CONSTRAINT company_mediation_patterns_company_id_fkey FOREIGN KEY (sdk_id) REFERENCES adtech.sdks(id);


--
-- Name: sdk_categories fk_category; Type: FK CONSTRAINT; Schema: adtech; Owner: postgres
--

ALTER TABLE ONLY adtech.sdk_categories
    ADD CONSTRAINT fk_category FOREIGN KEY (category_id) REFERENCES adtech.categories(id) ON DELETE CASCADE;


--
-- Name: companies fk_companies_parent; Type: FK CONSTRAINT; Schema: adtech; Owner: postgres
--

ALTER TABLE ONLY adtech.companies
    ADD CONSTRAINT fk_companies_parent FOREIGN KEY (parent_company_id) REFERENCES adtech.companies(id);


--
-- Name: company_developers fk_company_developers_category; Type: FK CONSTRAINT; Schema: adtech; Owner: postgres
--

ALTER TABLE ONLY adtech.company_developers
    ADD CONSTRAINT fk_company_developers_category FOREIGN KEY (developer_id) REFERENCES public.developers(id);


--
-- Name: companies fk_domain_id; Type: FK CONSTRAINT; Schema: adtech; Owner: postgres
--

ALTER TABLE ONLY adtech.companies
    ADD CONSTRAINT fk_domain_id FOREIGN KEY (domain_id) REFERENCES public.domains(id) ON DELETE CASCADE;


--
-- Name: sdk_categories fk_sdk; Type: FK CONSTRAINT; Schema: adtech; Owner: postgres
--

ALTER TABLE ONLY adtech.sdk_categories
    ADD CONSTRAINT fk_sdk FOREIGN KEY (sdk_id) REFERENCES adtech.sdks(id) ON DELETE CASCADE;


--
-- Name: sdk_packages sdk_packages_sdk_id_fkey; Type: FK CONSTRAINT; Schema: adtech; Owner: postgres
--

ALTER TABLE ONLY adtech.sdk_packages
    ADD CONSTRAINT sdk_packages_sdk_id_fkey FOREIGN KEY (sdk_id) REFERENCES adtech.sdks(id);


--
-- Name: sdk_paths sdk_paths_sdk_id_fkey; Type: FK CONSTRAINT; Schema: adtech; Owner: postgres
--

ALTER TABLE ONLY adtech.sdk_paths
    ADD CONSTRAINT sdk_paths_sdk_id_fkey FOREIGN KEY (sdk_id) REFERENCES adtech.sdks(id);


--
-- Name: sdks sdks_company_id_fkey; Type: FK CONSTRAINT; Schema: adtech; Owner: postgres
--

ALTER TABLE ONLY adtech.sdks
    ADD CONSTRAINT sdks_company_id_fkey FOREIGN KEY (company_id) REFERENCES adtech.companies(id);


--
-- Name: url_redirect_chains url_redirect_chains_api_call_id_fkey; Type: FK CONSTRAINT; Schema: adtech; Owner: postgres
--

ALTER TABLE ONLY adtech.url_redirect_chains
    ADD CONSTRAINT url_redirect_chains_api_call_id_fkey FOREIGN KEY (api_call_id) REFERENCES public.api_calls(id);


--
-- Name: url_redirect_chains url_redirect_chains_next_url_id_fkey; Type: FK CONSTRAINT; Schema: adtech; Owner: postgres
--

ALTER TABLE ONLY adtech.url_redirect_chains
    ADD CONSTRAINT url_redirect_chains_next_url_id_fkey FOREIGN KEY (next_url_id) REFERENCES adtech.urls(id);


--
-- Name: url_redirect_chains url_redirect_chains_run_id_fkey; Type: FK CONSTRAINT; Schema: adtech; Owner: postgres
--

ALTER TABLE ONLY adtech.url_redirect_chains
    ADD CONSTRAINT url_redirect_chains_run_id_fkey FOREIGN KEY (run_id) REFERENCES public.version_code_api_scan_results(id) ON DELETE CASCADE;


--
-- Name: url_redirect_chains url_redirect_chains_url_id_fkey; Type: FK CONSTRAINT; Schema: adtech; Owner: postgres
--

ALTER TABLE ONLY adtech.url_redirect_chains
    ADD CONSTRAINT url_redirect_chains_url_id_fkey FOREIGN KEY (url_id) REFERENCES adtech.urls(id);


--
-- Name: urls urls_domain_id_fkey; Type: FK CONSTRAINT; Schema: adtech; Owner: postgres
--

ALTER TABLE ONLY adtech.urls
    ADD CONSTRAINT urls_domain_id_fkey FOREIGN KEY (domain_id) REFERENCES public.domains(id);


--
-- Name: app_keyword_ranks_daily country_kr_fk; Type: FK CONSTRAINT; Schema: frontend; Owner: postgres
--

ALTER TABLE ONLY frontend.app_keyword_ranks_daily
    ADD CONSTRAINT country_kr_fk FOREIGN KEY (country) REFERENCES public.countries(id);


--
-- Name: store_app_ranks_daily fk_country; Type: FK CONSTRAINT; Schema: frontend; Owner: postgres
--

ALTER TABLE ONLY frontend.store_app_ranks_daily
    ADD CONSTRAINT fk_country FOREIGN KEY (country) REFERENCES public.countries(id);


--
-- Name: store_app_ranks_weekly fk_country; Type: FK CONSTRAINT; Schema: frontend; Owner: postgres
--

ALTER TABLE ONLY frontend.store_app_ranks_weekly
    ADD CONSTRAINT fk_country FOREIGN KEY (country) REFERENCES public.countries(id);


--
-- Name: store_app_ranks_daily fk_store_app; Type: FK CONSTRAINT; Schema: frontend; Owner: postgres
--

ALTER TABLE ONLY frontend.store_app_ranks_daily
    ADD CONSTRAINT fk_store_app FOREIGN KEY (store_app) REFERENCES public.store_apps(id);


--
-- Name: store_app_ranks_weekly fk_store_app; Type: FK CONSTRAINT; Schema: frontend; Owner: postgres
--

ALTER TABLE ONLY frontend.store_app_ranks_weekly
    ADD CONSTRAINT fk_store_app FOREIGN KEY (store_app) REFERENCES public.store_apps(id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: store_app_ranks_daily fk_store_category; Type: FK CONSTRAINT; Schema: frontend; Owner: postgres
--

ALTER TABLE ONLY frontend.store_app_ranks_daily
    ADD CONSTRAINT fk_store_category FOREIGN KEY (store_category) REFERENCES public.store_categories(id);


--
-- Name: store_app_ranks_weekly fk_store_category; Type: FK CONSTRAINT; Schema: frontend; Owner: postgres
--

ALTER TABLE ONLY frontend.store_app_ranks_weekly
    ADD CONSTRAINT fk_store_category FOREIGN KEY (store_category) REFERENCES public.store_categories(id);


--
-- Name: store_app_ranks_daily fk_store_collection; Type: FK CONSTRAINT; Schema: frontend; Owner: postgres
--

ALTER TABLE ONLY frontend.store_app_ranks_daily
    ADD CONSTRAINT fk_store_collection FOREIGN KEY (store_collection) REFERENCES public.store_collections(id);


--
-- Name: store_app_ranks_weekly fk_store_collection; Type: FK CONSTRAINT; Schema: frontend; Owner: postgres
--

ALTER TABLE ONLY frontend.store_app_ranks_weekly
    ADD CONSTRAINT fk_store_collection FOREIGN KEY (store_collection) REFERENCES public.store_collections(id);


--
-- Name: app_keyword_ranks_daily keyword_kr_fk; Type: FK CONSTRAINT; Schema: frontend; Owner: postgres
--

ALTER TABLE ONLY frontend.app_keyword_ranks_daily
    ADD CONSTRAINT keyword_kr_fk FOREIGN KEY (keyword_id) REFERENCES public.keywords(id);


--
-- Name: app_keyword_ranks_daily store_app_kr_fk; Type: FK CONSTRAINT; Schema: frontend; Owner: postgres
--

ALTER TABLE ONLY frontend.app_keyword_ranks_daily
    ADD CONSTRAINT store_app_kr_fk FOREIGN KEY (store_app) REFERENCES public.store_apps(id);


--
-- Name: app_keyword_ranks_daily store_kr_fk; Type: FK CONSTRAINT; Schema: frontend; Owner: postgres
--

ALTER TABLE ONLY frontend.app_keyword_ranks_daily
    ADD CONSTRAINT store_kr_fk FOREIGN KEY (store) REFERENCES public.stores(id);


--
-- Name: app_country_crawls app_country_crawls_app_fk; Type: FK CONSTRAINT; Schema: logging; Owner: postgres
--

ALTER TABLE ONLY logging.app_country_crawls
    ADD CONSTRAINT app_country_crawls_app_fk FOREIGN KEY (store_app) REFERENCES public.store_apps(id);


--
-- Name: app_country_crawls fk_country; Type: FK CONSTRAINT; Schema: logging; Owner: postgres
--

ALTER TABLE ONLY logging.app_country_crawls
    ADD CONSTRAINT fk_country FOREIGN KEY (country_id) REFERENCES public.countries(id);


--
-- Name: keywords_crawled_at keywords_crawled_at_fk; Type: FK CONSTRAINT; Schema: logging; Owner: postgres
--

ALTER TABLE ONLY logging.keywords_crawled_at
    ADD CONSTRAINT keywords_crawled_at_fk FOREIGN KEY (keyword) REFERENCES public.keywords(id);


--
-- Name: developers_crawled_at newtable_fk; Type: FK CONSTRAINT; Schema: logging; Owner: postgres
--

ALTER TABLE ONLY logging.developers_crawled_at
    ADD CONSTRAINT newtable_fk FOREIGN KEY (developer) REFERENCES public.developers(id);


--
-- Name: store_app_downloads store_app_download_fk; Type: FK CONSTRAINT; Schema: logging; Owner: postgres
--

ALTER TABLE ONLY logging.store_app_downloads
    ADD CONSTRAINT store_app_download_fk FOREIGN KEY (store_app) REFERENCES public.store_apps(id);


--
-- Name: store_app_sources store_app_sources_app_fk; Type: FK CONSTRAINT; Schema: logging; Owner: postgres
--

ALTER TABLE ONLY logging.store_app_sources
    ADD CONSTRAINT store_app_sources_app_fk FOREIGN KEY (store_app) REFERENCES public.store_apps(id) ON DELETE CASCADE;


--
-- Name: store_app_sources store_app_sources_store_fk; Type: FK CONSTRAINT; Schema: logging; Owner: postgres
--

ALTER TABLE ONLY logging.store_app_sources
    ADD CONSTRAINT store_app_sources_store_fk FOREIGN KEY (store) REFERENCES public.stores(id);


--
-- Name: store_app_waydroid_crawled_at waydroid_crawl_result_fk; Type: FK CONSTRAINT; Schema: logging; Owner: postgres
--

ALTER TABLE ONLY logging.store_app_waydroid_crawled_at
    ADD CONSTRAINT waydroid_crawl_result_fk FOREIGN KEY (crawl_result) REFERENCES public.crawl_results(id);


--
-- Name: store_app_waydroid_crawled_at waydroid_store_apps_crawl_fk; Type: FK CONSTRAINT; Schema: logging; Owner: postgres
--

ALTER TABLE ONLY logging.store_app_waydroid_crawled_at
    ADD CONSTRAINT waydroid_store_apps_crawl_fk FOREIGN KEY (store_app) REFERENCES public.store_apps(id);


--
-- Name: adstxt_crawl_results adstxt_crawl_results_d_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.adstxt_crawl_results
    ADD CONSTRAINT adstxt_crawl_results_d_fkey FOREIGN KEY (domain_id) REFERENCES public.domains(id);


--
-- Name: api_calls api_calls_ip_geo_snapshot_fk; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.api_calls
    ADD CONSTRAINT api_calls_ip_geo_snapshot_fk FOREIGN KEY (ip_geo_snapshot_id) REFERENCES public.ip_geo_snapshots(id);


--
-- Name: api_calls api_calls_run_fk; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.api_calls
    ADD CONSTRAINT api_calls_run_fk FOREIGN KEY (run_id) REFERENCES public.version_code_api_scan_results(id);


--
-- Name: api_calls api_calls_store_app_fk; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.api_calls
    ADD CONSTRAINT api_calls_store_app_fk FOREIGN KEY (store_app) REFERENCES public.store_apps(id) ON DELETE CASCADE;


--
-- Name: app_ads_entrys app_ads_entrys_domain_fk; Type: FK CONSTRAINT; Schema: public; Owner: james
--

ALTER TABLE ONLY public.app_ads_entrys
    ADD CONSTRAINT app_ads_entrys_domain_fk FOREIGN KEY (ad_domain) REFERENCES public.domains(id);


--
-- Name: app_ads_map app_ads_map_fk; Type: FK CONSTRAINT; Schema: public; Owner: james
--

ALTER TABLE ONLY public.app_ads_map
    ADD CONSTRAINT app_ads_map_fk FOREIGN KEY (app_ads_entry) REFERENCES public.app_ads_entrys(id);


--
-- Name: app_ads_map app_ads_map_fk_domain; Type: FK CONSTRAINT; Schema: public; Owner: james
--

ALTER TABLE ONLY public.app_ads_map
    ADD CONSTRAINT app_ads_map_fk_domain FOREIGN KEY (pub_domain) REFERENCES public.domains(id);


--
-- Name: app_country_metrics_history app_country_metrics_history_app_fk; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.app_country_metrics_history
    ADD CONSTRAINT app_country_metrics_history_app_fk FOREIGN KEY (store_app) REFERENCES public.store_apps(id);


--
-- Name: app_global_metrics_history app_global_metrics_history_app_fk; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.app_global_metrics_history
    ADD CONSTRAINT app_global_metrics_history_app_fk FOREIGN KEY (store_app) REFERENCES public.store_apps(id);


--
-- Name: app_urls_map app_urls_fk; Type: FK CONSTRAINT; Schema: public; Owner: james
--

ALTER TABLE ONLY public.app_urls_map
    ADD CONSTRAINT app_urls_fk FOREIGN KEY (store_app) REFERENCES public.store_apps(id);


--
-- Name: app_urls_map app_urls_map_fk_domain; Type: FK CONSTRAINT; Schema: public; Owner: james
--

ALTER TABLE ONLY public.app_urls_map
    ADD CONSTRAINT app_urls_map_fk_domain FOREIGN KEY (pub_domain) REFERENCES public.domains(id);


--
-- Name: crawl_scenario_country_config crawl_scenario_country_config_country_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.crawl_scenario_country_config
    ADD CONSTRAINT crawl_scenario_country_config_country_id_fkey FOREIGN KEY (country_id) REFERENCES public.countries(id);


--
-- Name: crawl_scenario_country_config crawl_scenario_country_config_scenario_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.crawl_scenario_country_config
    ADD CONSTRAINT crawl_scenario_country_config_scenario_id_fkey FOREIGN KEY (scenario_id) REFERENCES public.crawl_scenarios(id);


--
-- Name: creative_records creative_records_advertiser_app_fk; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.creative_records
    ADD CONSTRAINT creative_records_advertiser_app_fk FOREIGN KEY (advertiser_store_app_id) REFERENCES public.store_apps(id);


--
-- Name: creative_records creative_records_advertiser_domain_fk; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.creative_records
    ADD CONSTRAINT creative_records_advertiser_domain_fk FOREIGN KEY (advertiser_domain_id) REFERENCES public.domains(id) ON DELETE SET NULL;


--
-- Name: creative_records creative_records_api_call_fk; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.creative_records
    ADD CONSTRAINT creative_records_api_call_fk FOREIGN KEY (api_call_id) REFERENCES public.api_calls(id) ON DELETE CASCADE;


--
-- Name: creative_records creative_records_asset_fk; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.creative_records
    ADD CONSTRAINT creative_records_asset_fk FOREIGN KEY (creative_asset_id) REFERENCES public.creative_assets(id);


--
-- Name: creative_records creative_records_host_domain_fk; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.creative_records
    ADD CONSTRAINT creative_records_host_domain_fk FOREIGN KEY (creative_host_domain_id) REFERENCES public.domains(id) ON DELETE CASCADE;


--
-- Name: creative_records creative_records_initial_domain_fk; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.creative_records
    ADD CONSTRAINT creative_records_initial_domain_fk FOREIGN KEY (creative_initial_domain_id) REFERENCES public.domains(id) ON DELETE SET NULL;


--
-- Name: creative_records creative_records_mmp_domain_fk; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.creative_records
    ADD CONSTRAINT creative_records_mmp_domain_fk FOREIGN KEY (mmp_domain_id) REFERENCES public.domains(id) ON DELETE SET NULL;


--
-- Name: app_keywords_extracted description_keywords_app_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.app_keywords_extracted
    ADD CONSTRAINT description_keywords_app_id_fkey FOREIGN KEY (store_app) REFERENCES public.store_apps(id) ON DELETE CASCADE;


--
-- Name: app_keywords_extracted description_keywords_keyword_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.app_keywords_extracted
    ADD CONSTRAINT description_keywords_keyword_id_fkey FOREIGN KEY (keyword_id) REFERENCES public.keywords(id) ON DELETE CASCADE;


--
-- Name: developers developers_fk; Type: FK CONSTRAINT; Schema: public; Owner: james
--

ALTER TABLE ONLY public.developers
    ADD CONSTRAINT developers_fk FOREIGN KEY (store) REFERENCES public.stores(id);


--
-- Name: app_country_metrics_history fk_country; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.app_country_metrics_history
    ADD CONSTRAINT fk_country FOREIGN KEY (country_id) REFERENCES public.countries(id);


--
-- Name: ip_geo_snapshots fk_country; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.ip_geo_snapshots
    ADD CONSTRAINT fk_country FOREIGN KEY (country_id) REFERENCES public.countries(id);


--
-- Name: version_manifests fk_vm_version_code; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.version_manifests
    ADD CONSTRAINT fk_vm_version_code FOREIGN KEY (version_code) REFERENCES public.version_codes(id);


--
-- Name: store_apps_descriptions store_apps_descriptions_language_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.store_apps_descriptions
    ADD CONSTRAINT store_apps_descriptions_language_id_fkey FOREIGN KEY (language_id) REFERENCES public.languages(id) ON DELETE CASCADE;


--
-- Name: store_apps_descriptions store_apps_descriptions_store_app_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.store_apps_descriptions
    ADD CONSTRAINT store_apps_descriptions_store_app_fkey FOREIGN KEY (store_app) REFERENCES public.store_apps(id) ON DELETE CASCADE;


--
-- Name: store_apps store_apps_fk; Type: FK CONSTRAINT; Schema: public; Owner: james
--

ALTER TABLE ONLY public.store_apps
    ADD CONSTRAINT store_apps_fk FOREIGN KEY (store) REFERENCES public.stores(id);


--
-- Name: store_apps store_apps_fk_03; Type: FK CONSTRAINT; Schema: public; Owner: james
--

ALTER TABLE ONLY public.store_apps
    ADD CONSTRAINT store_apps_fk_03 FOREIGN KEY (crawl_result) REFERENCES public.crawl_results(id);


--
-- Name: store_apps store_apps_fk_1; Type: FK CONSTRAINT; Schema: public; Owner: james
--

ALTER TABLE ONLY public.store_apps
    ADD CONSTRAINT store_apps_fk_1 FOREIGN KEY (developer) REFERENCES public.developers(id);


--
-- Name: store_categories store_categories_fk; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.store_categories
    ADD CONSTRAINT store_categories_fk FOREIGN KEY (store) REFERENCES public.stores(id);


--
-- Name: store_collections store_collections_fk; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.store_collections
    ADD CONSTRAINT store_collections_fk FOREIGN KEY (store) REFERENCES public.stores(id);


--
-- Name: stores stores_fk; Type: FK CONSTRAINT; Schema: public; Owner: james
--

ALTER TABLE ONLY public.stores
    ADD CONSTRAINT stores_fk FOREIGN KEY (platform) REFERENCES public.platforms(id);


--
-- Name: version_codes vc_fk_store_app; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.version_codes
    ADD CONSTRAINT vc_fk_store_app FOREIGN KEY (store_app) REFERENCES public.store_apps(id);


--
-- Name: version_code_api_scan_results version_code_api_scan_results_version_code_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.version_code_api_scan_results
    ADD CONSTRAINT version_code_api_scan_results_version_code_id_fkey FOREIGN KEY (version_code_id) REFERENCES public.version_codes(id);


--
-- Name: version_code_sdk_scan_results version_code_sdk_scan_results_version_code_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.version_code_sdk_scan_results
    ADD CONSTRAINT version_code_sdk_scan_results_version_code_id_fkey FOREIGN KEY (version_code_id) REFERENCES public.version_codes(id);


--
-- Name: version_details_map version_details_map_string_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.version_details_map
    ADD CONSTRAINT version_details_map_string_id_fkey FOREIGN KEY (string_id) REFERENCES public.version_strings(id);


--
-- Name: SCHEMA public; Type: ACL; Schema: -; Owner: postgres
--

REVOKE USAGE ON SCHEMA public FROM PUBLIC;
GRANT ALL ON SCHEMA public TO PUBLIC;


--
-- PostgreSQL database dump complete
--

\unrestrict H8HMtkDkYggiiYeKcTWAfeZZbH0PdPaq1xAHjMcJbSMkJcdpS7FTANjZwyfksa7

