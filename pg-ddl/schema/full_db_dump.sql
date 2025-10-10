--
-- PostgreSQL database dump
--

\restrict K1BxpYbyx2EJLL836z6WNdegL3wuQBfU1Mks0njplly6hMOep67dilMhUzaEwna

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
-- Name: click_url_redirect_chains; Type: TABLE; Schema: adtech; Owner: postgres
--

CREATE TABLE adtech.click_url_redirect_chains (
    id integer NOT NULL,
    run_id integer NOT NULL,
    url text NOT NULL,
    redirect_url text NOT NULL
);


ALTER TABLE adtech.click_url_redirect_chains OWNER TO postgres;

--
-- Name: click_url_redirect_chains_id_seq; Type: SEQUENCE; Schema: adtech; Owner: postgres
--

CREATE SEQUENCE adtech.click_url_redirect_chains_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE adtech.click_url_redirect_chains_id_seq OWNER TO postgres;

--
-- Name: click_url_redirect_chains_id_seq; Type: SEQUENCE OWNED BY; Schema: adtech; Owner: postgres
--

ALTER SEQUENCE adtech.click_url_redirect_chains_id_seq OWNED BY adtech.click_url_redirect_chains.id;


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
-- Name: sdk_categories; Type: TABLE; Schema: adtech; Owner: postgres
--

CREATE TABLE adtech.sdk_categories (
    sdk_id integer NOT NULL,
    category_id integer NOT NULL
);


ALTER TABLE adtech.sdk_categories OWNER TO postgres;

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
-- Name: sdk_packages; Type: TABLE; Schema: adtech; Owner: postgres
--

CREATE TABLE adtech.sdk_packages (
    id integer NOT NULL,
    package_pattern character varying NOT NULL,
    sdk_id integer
);


ALTER TABLE adtech.sdk_packages OWNER TO postgres;

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
-- Name: company_value_string_mapping; Type: MATERIALIZED VIEW; Schema: adtech; Owner: postgres
--

CREATE MATERIALIZED VIEW adtech.company_value_string_mapping AS
 SELECT vd.id AS version_string_id,
    sd.company_id,
    sp.sdk_id AS id
   FROM ((public.version_strings vd
     JOIN adtech.sdk_packages sp ON ((vd.value_name ~~* ((sp.package_pattern)::text || '%'::text))))
     LEFT JOIN adtech.sdks sd ON ((sp.sdk_id = sd.id)))
  WITH NO DATA;


ALTER MATERIALIZED VIEW adtech.company_value_string_mapping OWNER TO postgres;

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
    textsearchable_index_col tsvector GENERATED ALWAYS AS (to_tsvector('simple'::regconfig, (COALESCE(name, ''::character varying))::text)) STORED,
    additional_html_scraped_at timestamp without time zone,
    icon_url_100 text
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
-- Name: store_apps_companies_sdk; Type: MATERIALIZED VIEW; Schema: adtech; Owner: postgres
--

CREATE MATERIALIZED VIEW adtech.store_apps_companies_sdk AS
 WITH latest_version_codes AS (
         SELECT DISTINCT ON (version_codes.store_app) version_codes.id,
            version_codes.store_app,
            version_codes.version_code
           FROM public.version_codes
          WHERE (version_codes.crawl_result = 1)
          ORDER BY version_codes.store_app, (string_to_array((version_codes.version_code)::text, '.'::text))::bigint[] DESC
        ), sdk_apps_with_companies AS (
         SELECT DISTINCT vc.store_app,
            cvsm.company_id,
            COALESCE(pc.parent_company_id, cvsm.company_id) AS parent_id
           FROM (((latest_version_codes vc
             LEFT JOIN public.version_details_map vdm ON ((vc.id = vdm.version_code)))
             JOIN adtech.company_value_string_mapping cvsm ON ((vdm.string_id = cvsm.version_string_id)))
             LEFT JOIN adtech.companies pc ON ((cvsm.company_id = pc.id)))
        ), sdk_paths_with_companies AS (
         SELECT DISTINCT vc.store_app,
            sd.company_id,
            COALESCE(pc.parent_company_id, sd.company_id) AS parent_id
           FROM (((((latest_version_codes vc
             LEFT JOIN public.version_details_map vdm ON ((vc.id = vdm.version_code)))
             LEFT JOIN public.version_strings vs ON ((vdm.string_id = vs.id)))
             JOIN adtech.sdk_paths ptm ON ((vs.value_name ~~* ((ptm.path_pattern)::text || '%'::text))))
             LEFT JOIN adtech.sdks sd ON ((ptm.sdk_id = sd.id)))
             LEFT JOIN adtech.companies pc ON ((sd.company_id = pc.id)))
        ), dev_apps_with_companies AS (
         SELECT DISTINCT sa.id AS store_app,
            cd.company_id,
            COALESCE(pc.parent_company_id, cd.company_id) AS parent_id
           FROM ((adtech.company_developers cd
             LEFT JOIN public.store_apps sa ON ((cd.developer_id = sa.developer)))
             LEFT JOIN adtech.companies pc ON ((cd.company_id = pc.id)))
        ), all_apps_with_companies AS (
         SELECT sawc.store_app,
            sawc.company_id,
            sawc.parent_id
           FROM sdk_apps_with_companies sawc
        UNION
         SELECT spwc.store_app,
            spwc.company_id,
            spwc.parent_id
           FROM sdk_paths_with_companies spwc
        UNION
         SELECT dawc.store_app,
            dawc.company_id,
            dawc.parent_id
           FROM dev_apps_with_companies dawc
        ), distinct_apps_with_cats AS (
         SELECT DISTINCT aawc.store_app,
            c.id AS category_id
           FROM ((all_apps_with_companies aawc
             LEFT JOIN adtech.company_categories cc ON ((aawc.company_id = cc.company_id)))
             LEFT JOIN adtech.categories c ON ((cc.category_id = c.id)))
        ), distinct_store_apps AS (
         SELECT DISTINCT lvc.store_app
           FROM latest_version_codes lvc
        ), all_combinations AS (
         SELECT sa.store_app,
            c.id AS category_id
           FROM (distinct_store_apps sa
             CROSS JOIN adtech.categories c)
        ), unmatched_apps AS (
         SELECT DISTINCT ac.store_app,
            (- ac.category_id) AS company_id,
            (- ac.category_id) AS parent_id
           FROM (all_combinations ac
             LEFT JOIN distinct_apps_with_cats dawc ON (((ac.store_app = dawc.store_app) AND (ac.category_id = dawc.category_id))))
          WHERE (dawc.store_app IS NULL)
        ), final_union AS (
         SELECT aawc.store_app,
            aawc.company_id,
            aawc.parent_id
           FROM all_apps_with_companies aawc
        UNION
         SELECT ua.store_app,
            ua.company_id,
            ua.parent_id
           FROM unmatched_apps ua
        )
 SELECT store_app,
    company_id,
    parent_id
   FROM final_union
  WITH NO DATA;


ALTER MATERIALIZED VIEW adtech.store_apps_companies_sdk OWNER TO postgres;

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
-- Name: store_apps_country_history; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.store_apps_country_history (
    store_app integer NOT NULL,
    review_count double precision,
    rating double precision,
    crawled_date date,
    rating_count integer,
    histogram bigint[] DEFAULT ARRAY[]::integer[],
    installs bigint,
    id integer NOT NULL,
    country_id smallint,
    store_last_updated date
);


ALTER TABLE public.store_apps_country_history OWNER TO postgres;

--
-- Name: store_apps_history_weekly; Type: MATERIALIZED VIEW; Schema: public; Owner: postgres
--

CREATE MATERIALIZED VIEW public.store_apps_history_weekly AS
 WITH date_diffs AS (
         SELECT sach.store_app,
            sach.country_id,
            sach.crawled_date,
            sach.installs,
            sach.rating_count,
            max(sach.crawled_date) OVER (PARTITION BY sach.store_app, sach.country_id) AS last_date,
            (sach.installs - lead(sach.installs) OVER (PARTITION BY sach.store_app, sach.country_id ORDER BY sach.crawled_date DESC)) AS installs_diff,
            (sach.rating_count - lead(sach.rating_count) OVER (PARTITION BY sach.store_app, sach.country_id ORDER BY sach.crawled_date DESC)) AS rating_count_diff,
            (sach.crawled_date - lead(sach.crawled_date) OVER (PARTITION BY sach.store_app, sach.country_id ORDER BY sach.crawled_date DESC)) AS days_diff
           FROM public.store_apps_country_history sach
          WHERE ((sach.store_app IN ( SELECT sa.id
                   FROM public.store_apps sa
                  WHERE (sa.crawl_result = 1))) AND (sach.crawled_date > (CURRENT_DATE - '375 days'::interval)))
        ), weekly_totals AS (
         SELECT (date_trunc('week'::text, (date_diffs.crawled_date)::timestamp with time zone))::date AS week_start,
            date_diffs.store_app,
            date_diffs.country_id,
            sum(date_diffs.installs_diff) AS installs_diff,
            sum(date_diffs.rating_count_diff) AS rating_count_diff,
            sum(date_diffs.days_diff) AS days_diff
           FROM date_diffs
          GROUP BY ((date_trunc('week'::text, (date_diffs.crawled_date)::timestamp with time zone))::date), date_diffs.store_app, date_diffs.country_id
        )
 SELECT week_start,
    store_app,
    country_id,
    installs_diff,
    rating_count_diff
   FROM weekly_totals
  ORDER BY week_start DESC, store_app, country_id
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
         SELECT store_apps_history_weekly.store_app,
            max(store_apps_history_weekly.week_start) AS app_max_week
           FROM public.store_apps_history_weekly
          WHERE (store_apps_history_weekly.country_id = 840)
          GROUP BY store_apps_history_weekly.store_app
        ), baseline_period AS (
         SELECT store_apps_history_weekly.store_app,
            avg(store_apps_history_weekly.installs_diff) AS avg_installs_diff,
            stddev(store_apps_history_weekly.installs_diff) AS stddev_installs_diff,
            avg(store_apps_history_weekly.rating_count_diff) AS avg_rating_diff,
            stddev(store_apps_history_weekly.rating_count_diff) AS stddev_rating_diff
           FROM public.store_apps_history_weekly,
            latest_week
          WHERE ((store_apps_history_weekly.week_start >= (latest_week.max_week - '84 days'::interval)) AND (store_apps_history_weekly.week_start <= (latest_week.max_week - '35 days'::interval)) AND (store_apps_history_weekly.country_id = 840))
          GROUP BY store_apps_history_weekly.store_app
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
           FROM ((public.store_apps_history_weekly s
             CROSS JOIN latest_week lw)
             JOIN latest_week_per_app lwpa ON ((s.store_app = lwpa.store_app)))
          WHERE ((s.week_start >= (lw.max_week - '28 days'::interval)) AND (s.country_id = 840))
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
                    ELSE (0)::bigint
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
                    ELSE (0)::bigint
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
                    ELSE (0)::bigint
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
                    ELSE (0)::bigint
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
        )
 SELECT sa.id,
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
   FROM (((((((((((((((public.store_apps sa
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
        ), sdk_based_companies AS (
         SELECT sac.store_app,
            cm.mapped_category AS app_category,
            sac.company_id,
            sac.parent_id,
            ad_1.domain_name AS ad_domain,
            'sdk'::text AS tag_source
           FROM ((((adtech.store_apps_companies_sdk sac
             LEFT JOIN adtech.companies c_1 ON ((sac.company_id = c_1.id)))
             LEFT JOIN public.domains ad_1 ON ((c_1.domain_id = ad_1.id)))
             LEFT JOIN public.store_apps sa_1 ON ((sac.store_app = sa_1.id)))
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
          WHERE (cr_1.mmp_domain_id IS NOT NULL)
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
             LEFT JOIN public.domains ad ON ((cr_1.mmp_domain_id = ad.id)))
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
  WHERE (vcasr.run_at >= (now() - '1 mon'::interval))
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
-- Name: apps_new_monthly; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.apps_new_monthly AS
 WITH rankedapps AS (
         SELECT sa_1.id,
            sa_1.name,
            sa_1.store_id,
            sa_1.store,
            sa_1.category,
            sa_1.rating,
            sa_1.review_count,
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
          WHERE ((sa_1.release_date >= (CURRENT_DATE - '30 days'::interval)) AND (sa_1.created_at >= (CURRENT_DATE - '45 days'::interval)) AND (sa_1.crawl_result = 1))
        )
 SELECT ra.id,
    ra.name,
    ra.store_id,
    ra.store,
    ra.category,
    ra.rating,
    ra.review_count,
    ra.installs,
    ra.installs_sum_1w,
    ra.installs_sum_4w,
    ra.ratings_sum_1w,
    ra.ratings_sum_4w,
    ra.store_last_updated,
    ra.ad_supported,
    ra.in_app_purchases,
    ra.created_at,
    ra.updated_at,
    ra.crawl_result,
    sa.icon_url_100,
    ra.icon_url_512,
    ra.release_date,
    ra.rating_count,
    ra.featured_image_url,
    ra.phone_image_url_1,
    ra.phone_image_url_2,
    ra.phone_image_url_3,
    ra.tablet_image_url_1,
    ra.tablet_image_url_2,
    ra.tablet_image_url_3,
    ra.category AS app_category,
    ra.rn
   FROM (rankedapps ra
     LEFT JOIN public.store_apps sa ON ((ra.id = sa.id)))
  WHERE (ra.rn <= 100)
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
            sa_1.rating,
            sa_1.review_count,
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
          WHERE ((sa_1.release_date >= (CURRENT_DATE - '7 days'::interval)) AND (sa_1.created_at >= (CURRENT_DATE - '11 days'::interval)) AND (sa_1.crawl_result = 1))
        )
 SELECT ra.id,
    ra.name,
    ra.store_id,
    ra.store,
    ra.category,
    ra.rating,
    ra.review_count,
    ra.installs,
    ra.installs_sum_1w,
    ra.installs_sum_4w,
    ra.ratings_sum_1w,
    ra.ratings_sum_4w,
    ra.store_last_updated,
    ra.ad_supported,
    ra.in_app_purchases,
    ra.created_at,
    ra.updated_at,
    ra.crawl_result,
    sa.icon_url_100,
    ra.icon_url_512,
    ra.release_date,
    ra.rating_count,
    ra.featured_image_url,
    ra.phone_image_url_1,
    ra.phone_image_url_2,
    ra.phone_image_url_3,
    ra.tablet_image_url_1,
    ra.tablet_image_url_2,
    ra.tablet_image_url_3,
    ra.category AS app_category,
    ra.rn
   FROM (rankedapps ra
     LEFT JOIN public.store_apps sa ON ((ra.id = sa.id)))
  WHERE (ra.rn <= 100)
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
            sa_1.rating,
            sa_1.review_count,
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
 SELECT ra.id,
    ra.name,
    ra.store_id,
    ra.store,
    ra.category,
    ra.rating,
    ra.review_count,
    ra.installs,
    ra.installs_sum_1w,
    ra.installs_sum_4w,
    ra.ratings_sum_1w,
    ra.ratings_sum_4w,
    ra.store_last_updated,
    ra.ad_supported,
    ra.in_app_purchases,
    ra.created_at,
    ra.updated_at,
    ra.crawl_result,
    sa.icon_url_100,
    ra.icon_url_512,
    ra.release_date,
    ra.rating_count,
    ra.featured_image_url,
    ra.phone_image_url_1,
    ra.phone_image_url_2,
    ra.phone_image_url_3,
    ra.tablet_image_url_1,
    ra.tablet_image_url_2,
    ra.tablet_image_url_3,
    ra.category AS app_category,
    ra.rn
   FROM (rankedapps ra
     LEFT JOIN public.store_apps sa ON ((ra.id = sa.id)))
  WHERE (ra.rn <= 100)
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
           FROM public.store_apps_history_weekly sahw
          WHERE ((sahw.week_start > (CURRENT_DATE - '31 days'::interval)) AND (sahw.country_id = 840) AND ((sahw.installs_diff > (0)::numeric) OR (sahw.rating_count_diff > 0)))
          GROUP BY sahw.store_app
        ), distinct_apps_group AS (
         SELECT sa.store,
            csac.store_app,
            csac.app_category,
            tag.tag_source,
            sa.installs,
            sa.rating_count
           FROM ((adtech.combined_store_apps_companies csac
             LEFT JOIN public.store_apps sa ON ((csac.store_app = sa.id)))
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
-- Name: store_apps_version_details; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.store_apps_version_details AS
 WITH latest_version_codes AS (
         SELECT DISTINCT ON (version_codes.store_app) version_codes.id,
            version_codes.store_app,
            version_codes.version_code,
            version_codes.updated_at,
            version_codes.crawl_result
           FROM public.version_codes
          ORDER BY version_codes.store_app, (string_to_array((version_codes.version_code)::text, '.'::text))::bigint[] DESC
        )
 SELECT DISTINCT vs.id AS version_string_id,
    sa.store,
    sa.store_id,
    cvsm.company_id,
    c.name AS company_name,
    ad.domain_name AS company_domain,
    cats.url_slug AS category_slug
   FROM ((((((((latest_version_codes vc
     LEFT JOIN public.version_details_map vdm ON ((vc.id = vdm.version_code)))
     LEFT JOIN public.version_strings vs ON ((vdm.string_id = vs.id)))
     LEFT JOIN adtech.company_value_string_mapping cvsm ON ((vs.id = cvsm.version_string_id)))
     LEFT JOIN adtech.companies c ON ((cvsm.company_id = c.id)))
     LEFT JOIN adtech.company_categories cc ON ((c.id = cc.company_id)))
     LEFT JOIN adtech.categories cats ON ((cc.category_id = cats.id)))
     LEFT JOIN public.domains ad ON ((c.domain_id = ad.id)))
     LEFT JOIN public.store_apps sa ON ((vc.store_app = sa.id)))
  WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.store_apps_version_details OWNER TO postgres;

--
-- Name: companies_apps_overview; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.companies_apps_overview AS
 SELECT DISTINCT store_id,
    company_id,
    company_name,
    company_domain,
    category_slug
   FROM frontend.store_apps_version_details
  WHERE (company_id IS NOT NULL)
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
           FROM public.store_apps_history_weekly sahw
          WHERE ((sahw.week_start > (CURRENT_DATE - '31 days'::interval)) AND (sahw.country_id = 840) AND ((sahw.installs_diff > (0)::numeric) OR (sahw.rating_count_diff > 0)))
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
             LEFT JOIN public.store_apps sa ON ((csac.store_app = sa.id)))
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
           FROM public.store_apps_history_weekly sahw
          WHERE ((sahw.week_start > (CURRENT_DATE - '31 days'::interval)) AND (sahw.country_id = 840) AND ((sahw.installs_diff > (0)::numeric) OR (sahw.rating_count_diff > 0)))
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
             LEFT JOIN public.store_apps sa ON ((csac.store_app = sa.id)))
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
           FROM public.store_apps_history_weekly sahw
          WHERE ((sahw.week_start > (CURRENT_DATE - '31 days'::interval)) AND (sahw.country_id = 840) AND ((sahw.installs_diff > (0)::numeric) OR (sahw.rating_count_diff > 0)))
          GROUP BY sahw.store_app
        )
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
     LEFT JOIN public.store_apps sa ON ((csac.store_app = sa.id)))
     LEFT JOIN d30_counts dc ON ((csac.store_app = dc.store_app)))
     LEFT JOIN adtech.company_categories ccats ON ((csac.company_id = ccats.company_id)))
     LEFT JOIN adtech.categories cats ON ((ccats.category_id = cats.id)))
     CROSS JOIN LATERAL ( VALUES ('sdk'::text,csac.sdk), ('api_call'::text,csac.api_call), ('app_ads_direct'::text,csac.app_ads_direct), ('app_ads_reseller'::text,csac.app_ads_reseller)) tag(tag_source, present))
  WHERE (tag.present IS TRUE)
  GROUP BY sa.store, csac.app_category, tag.tag_source, csac.ad_domain, c.name,
        CASE
            WHEN (tag.tag_source ~~ 'app_ads%'::text) THEN 'ad-networks'::character varying
            ELSE cats.url_slug
        END
  WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.companies_category_tag_type_stats OWNER TO postgres;

--
-- Name: companies_creative_rankings; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.companies_creative_rankings AS
 WITH creative_rankings AS (
         SELECT ca.file_extension,
            cr.advertiser_store_app_id,
            cr.creative_initial_domain_id,
            cr.creative_host_domain_id,
            cr.additional_ad_domain_ids,
            vcasr.run_at,
            ca.md5_hash,
            COALESCE(ca.phash, ca.md5_hash) AS vhash
           FROM (((public.creative_records cr
             LEFT JOIN public.creative_assets ca ON ((cr.creative_asset_id = ca.id)))
             LEFT JOIN public.api_calls ac ON ((cr.api_call_id = ac.id)))
             LEFT JOIN public.version_code_api_scan_results vcasr ON ((ac.run_id = vcasr.id)))
        ), combined_domains AS (
         SELECT cr.vhash,
            cr.md5_hash,
            cr.file_extension,
            cr.creative_initial_domain_id AS domain_id,
            cr.advertiser_store_app_id,
            cr.run_at
           FROM creative_rankings cr
        UNION
         SELECT cr.vhash,
            cr.md5_hash,
            cr.file_extension,
            cr.creative_host_domain_id,
            cr.advertiser_store_app_id,
            cr.run_at
           FROM creative_rankings cr
        UNION
         SELECT cr.vhash,
            cr.md5_hash,
            cr.file_extension,
            unnest(cr.additional_ad_domain_ids) AS unnest,
            cr.advertiser_store_app_id,
            cr.run_at
           FROM creative_rankings cr
        ), visually_distinct AS (
         SELECT cdm.company_id,
            cd.file_extension,
            cd.advertiser_store_app_id,
            cd.vhash,
            min((cd.md5_hash)::text) AS md5_hash,
            max(cd.run_at) AS last_seen
           FROM (combined_domains cd
             LEFT JOIN adtech.company_domain_mapping cdm ON ((cd.domain_id = cdm.domain_id)))
          GROUP BY cdm.company_id, cd.file_extension, cd.advertiser_store_app_id, cd.vhash
        )
 SELECT DISTINCT vd.company_id,
    vd.md5_hash,
    vd.file_extension,
    ad.domain_name AS company_domain,
    sa.name AS advertiser_name,
    sa.store,
    sa.store_id AS advertiser_store_id,
    sa.icon_url_100,
    sa.icon_url_512,
    sa.installs,
    sa.rating_count,
    sa.rating,
    sa.installs_sum_1w,
    sa.ratings_sum_1w,
    sa.installs_sum_4w,
    sa.ratings_sum_4w,
    vd.last_seen
   FROM (((visually_distinct vd
     LEFT JOIN adtech.companies c ON ((vd.company_id = c.id)))
     LEFT JOIN public.domains ad ON ((c.domain_id = ad.id)))
     LEFT JOIN frontend.store_apps_overview sa ON ((vd.advertiser_store_app_id = sa.id)))
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
           FROM public.store_apps_history_weekly sahw
          WHERE ((sahw.week_start > (CURRENT_DATE - '31 days'::interval)) AND (sahw.country_id = 840) AND ((sahw.installs_diff > (0)::numeric) OR (sahw.rating_count_diff > 0)))
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
             LEFT JOIN public.store_apps sa ON ((csac.store_app = sa.id)))
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
           FROM public.store_apps_history_weekly sahw
          WHERE ((sahw.week_start > (CURRENT_DATE - '31 days'::interval)) AND (sahw.country_id = 840) AND ((sahw.installs_diff > (0)::numeric) OR (sahw.rating_count_diff > 0)))
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
             LEFT JOIN public.store_apps sa ON ((csac.store_app = sa.id)))
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
-- Name: companies_version_details_count; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.companies_version_details_count AS
 SELECT savd.store,
    savd.company_name,
    savd.company_domain,
    vs.xml_path,
    vs.value_name,
    count(DISTINCT savd.store_id) AS app_count
   FROM (frontend.store_apps_version_details savd
     LEFT JOIN public.version_strings vs ON ((savd.version_string_id = vs.id)))
  GROUP BY savd.store, savd.company_name, savd.company_domain, vs.xml_path, vs.value_name
  ORDER BY (count(DISTINCT savd.store_id)) DESC
 LIMIT 1000
  WITH NO DATA;


ALTER MATERIALIZED VIEW frontend.companies_version_details_count OWNER TO postgres;

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
-- Name: keyword_scores; Type: MATERIALIZED VIEW; Schema: frontend; Owner: postgres
--

CREATE MATERIALIZED VIEW frontend.keyword_scores AS
 WITH latest_en_descriptions AS (
         SELECT DISTINCT ON (sad.store_app) sad.store_app,
            sad.id AS description_id
           FROM (public.store_apps_descriptions sad
             JOIN public.description_keywords dk ON ((sad.id = dk.description_id)))
          WHERE (sad.language_id = 1)
          ORDER BY sad.store_app, sad.updated_at DESC
        ), keyword_app_counts AS (
         SELECT sa.store,
            k.keyword_text,
            dk.keyword_id,
            count(DISTINCT led.store_app) AS app_count
           FROM (((latest_en_descriptions led
             LEFT JOIN public.description_keywords dk ON ((led.description_id = dk.description_id)))
             LEFT JOIN public.keywords k ON ((dk.keyword_id = k.id)))
             LEFT JOIN public.store_apps sa ON ((led.store_app = sa.id)))
          WHERE (dk.keyword_id IS NOT NULL)
          GROUP BY sa.store, k.keyword_text, dk.keyword_id
        ), total_app_count AS (
         SELECT sa.store,
            count(*) AS total_apps
           FROM (latest_en_descriptions led
             LEFT JOIN public.store_apps sa ON ((led.store_app = sa.id)))
          GROUP BY sa.store
        )
 SELECT kac.store,
    kac.keyword_text,
    kac.keyword_id,
    kac.app_count,
    tac.total_apps,
    round(((100)::numeric * (((1)::double precision - (ln(((tac.total_apps)::double precision / ((kac.app_count + 1))::double precision)) / ln((tac.total_apps)::double precision))))::numeric), 2) AS competitiveness_score
   FROM (keyword_app_counts kac
     LEFT JOIN total_app_count tac ON ((kac.store = tac.store)))
  ORDER BY (round(((100)::numeric * (((1)::double precision - (ln(((tac.total_apps)::double precision / ((kac.app_count + 1))::double precision)) / ln((tac.total_apps)::double precision))))::numeric), 2)) DESC
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
             LEFT JOIN public.store_apps sa ON ((lvc.store_app = sa.id)))
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
    sa.installs,
    sa.rating_count,
    sa.rating,
    sa.review_count,
    sa.installs_sum_1w,
    sa.installs_sum_4w,
    sa.ratings_sum_1w,
    sa.ratings_sum_4w,
    sa.icon_url_100,
    sa.icon_url_512,
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
 WITH ranked_z_scores AS (
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
            sa.textsearchable_index_col,
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
           FROM ((public.store_app_z_scores saz
             LEFT JOIN public.store_apps sa ON ((saz.store_app = sa.id)))
             LEFT JOIN public.category_mapping cm ON (((sa.category)::text = (cm.original_category)::text)))
        )
 SELECT store,
    store_id,
    name AS app_name,
    mapped_category AS app_category,
    in_app_purchases,
    ad_supported,
    icon_url_512,
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
-- Name: store_apps_audit; Type: TABLE; Schema: logging; Owner: postgres
--

CREATE TABLE logging.store_apps_audit (
    operation character(1) NOT NULL,
    stamp timestamp without time zone NOT NULL,
    userid text NOT NULL,
    row_id bigint NOT NULL,
    store smallint NOT NULL,
    store_id text NOT NULL,
    crawl_result integer
);


ALTER TABLE logging.store_apps_audit OWNER TO postgres;

--
-- Name: store_apps_crawl; Type: TABLE; Schema: logging; Owner: postgres
--

CREATE TABLE logging.store_apps_crawl (
    index bigint,
    crawl_result bigint,
    store bigint,
    store_id text,
    store_app bigint,
    crawled_at timestamp without time zone
);


ALTER TABLE logging.store_apps_crawl OWNER TO postgres;

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
-- Name: ad_domains_old; Type: TABLE; Schema: public; Owner: james
--

CREATE TABLE public.ad_domains_old (
    id integer NOT NULL,
    domain character varying NOT NULL,
    created_at timestamp without time zone DEFAULT timezone('utc'::text, now()),
    updated_at timestamp without time zone DEFAULT timezone('utc'::text, now())
);


ALTER TABLE public.ad_domains_old OWNER TO james;

--
-- Name: ad_domains_id_seq; Type: SEQUENCE; Schema: public; Owner: james
--

ALTER TABLE public.ad_domains_old ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
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
-- Name: audit_dates; Type: MATERIALIZED VIEW; Schema: public; Owner: postgres
--

CREATE MATERIALIZED VIEW public.audit_dates AS
 WITH sa AS (
         SELECT (store_apps_audit.stamp)::date AS updated_date,
            'store_apps'::text AS table_name,
            count(*) AS updated_count
           FROM logging.store_apps_audit
          GROUP BY ((store_apps_audit.stamp)::date)
        )
 SELECT updated_date,
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
           FROM (((public.app_urls_map aum_1
             LEFT JOIN public.domains pd_1 ON ((aum_1.pub_domain = pd_1.id)))
             LEFT JOIN public.store_apps sa_1 ON ((aum_1.store_app = sa_1.id)))
             LEFT JOIN public.developers d_1 ON ((sa_1.developer = d_1.id)))
        )
 SELECT sa.store,
    sa.store_id,
    sa.name,
    sa.icon_url_512,
    sa.installs,
    sa.rating,
    sa.rating_count,
    sa.review_count,
    d.name AS developer_name,
    pd.domain_name AS developer_url,
    d.store AS developer_store,
    pd.id AS domain_id,
    d.developer_id
   FROM (((public.store_apps sa
     LEFT JOIN public.developers d ON ((sa.developer = d.id)))
     LEFT JOIN public.app_urls_map aum ON ((sa.id = aum.store_app)))
     LEFT JOIN public.domains pd ON ((aum.pub_domain = pd.id)))
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
-- Name: pub_domains_old; Type: TABLE; Schema: public; Owner: james
--

CREATE TABLE public.pub_domains_old (
    id integer NOT NULL,
    url character varying NOT NULL,
    created_at timestamp without time zone DEFAULT timezone('utc'::text, now()),
    updated_at timestamp without time zone DEFAULT timezone('utc'::text, now()),
    crawl_result integer,
    crawled_at timestamp without time zone
);


ALTER TABLE public.pub_domains_old OWNER TO james;

--
-- Name: pub_domains_id_seq; Type: SEQUENCE; Schema: public; Owner: james
--

ALTER TABLE public.pub_domains_old ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.pub_domains_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: store_app_z_scores_history; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.store_app_z_scores_history (
    target_week date NOT NULL,
    store character varying NOT NULL,
    store_app numeric NOT NULL,
    store_id character varying NOT NULL,
    app_name character varying NOT NULL,
    app_category character varying,
    in_app_purchases boolean,
    ad_supported boolean,
    icon_url_100 text,
    icon_url_512 text,
    installs bigint,
    rating_count bigint,
    installs_sum_1w numeric,
    ratings_sum_1w numeric,
    baseline_installs_2w numeric,
    baseline_ratings_2w numeric,
    installs_pct_increase numeric,
    ratings_pct_increase numeric,
    installs_z_score_1w numeric,
    ratings_z_score_1w numeric
);


ALTER TABLE public.store_app_z_scores_history OWNER TO postgres;

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
         SELECT num_series.store,
            (generate_series((CURRENT_DATE - '365 days'::interval), (CURRENT_DATE)::timestamp without time zone, '1 day'::interval))::date AS date
           FROM generate_series(1, 2, 1) num_series(store)
        ), created_dates AS (
         SELECT sa.store,
            (sa.created_at)::date AS created_date,
            sas.crawl_source,
            count(*) AS created_count
           FROM (public.store_apps sa
             LEFT JOIN logging.store_app_sources sas ON (((sa.id = sas.store_app) AND (sa.store = sas.store))))
          WHERE (sa.created_at >= (CURRENT_DATE - '365 days'::interval))
          GROUP BY sa.store, ((sa.created_at)::date), sas.crawl_source
        )
 SELECT my_dates.store,
    my_dates.date,
    created_dates.crawl_source,
    created_dates.created_count
   FROM (my_dates
     LEFT JOIN created_dates ON (((my_dates.date = created_dates.created_date) AND (my_dates.store = created_dates.store))))
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
         SELECT sa.id AS store_app,
            sa.store,
            sa.store_last_updated,
            sa.name,
            sa.installs,
            sa.rating_count,
            sa.store_id
           FROM (frontend.store_apps_z_scores saz
             LEFT JOIN public.store_apps sa ON (((saz.store_id)::text = (sa.store_id)::text)))
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
             LEFT JOIN public.store_apps sa ON ((ar.store_app = sa.id)))
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
-- Name: store_apps_updated_at; Type: MATERIALIZED VIEW; Schema: public; Owner: postgres
--

CREATE MATERIALIZED VIEW public.store_apps_updated_at AS
 WITH my_dates AS (
         SELECT num_series.store,
            (generate_series((CURRENT_DATE - '365 days'::interval), (CURRENT_DATE)::timestamp without time zone, '1 day'::interval))::date AS date
           FROM generate_series(1, 2, 1) num_series(store)
        ), updated_dates AS (
         SELECT store_apps.store,
            (store_apps.updated_at)::date AS last_updated_date,
            count(*) AS last_updated_count
           FROM public.store_apps
          WHERE (store_apps.updated_at >= (CURRENT_DATE - '365 days'::interval))
          GROUP BY store_apps.store, ((store_apps.updated_at)::date)
        )
 SELECT my_dates.store,
    my_dates.date,
    updated_dates.last_updated_count,
    audit_dates.updated_count
   FROM ((my_dates
     LEFT JOIN updated_dates ON (((my_dates.date = updated_dates.last_updated_date) AND (my_dates.store = updated_dates.store))))
     LEFT JOIN public.audit_dates ON ((my_dates.date = audit_dates.updated_date)))
  ORDER BY my_dates.date DESC
  WITH NO DATA;


ALTER MATERIALIZED VIEW public.store_apps_updated_at OWNER TO postgres;

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
-- Name: top_categories; Type: MATERIALIZED VIEW; Schema: public; Owner: postgres
--

CREATE MATERIALIZED VIEW public.top_categories AS
 WITH rankedapps AS (
         SELECT sa.id,
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
            row_number() OVER (PARTITION BY sa.store, cm.mapped_category ORDER BY sa.installs DESC NULLS LAST, sa.rating_count DESC NULLS LAST) AS rn
           FROM (public.store_apps sa
             JOIN public.category_mapping cm ON (((sa.category)::text = (cm.original_category)::text)))
          WHERE (sa.crawl_result = 1)
        )
 SELECT id,
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
-- Name: user_requested_scan; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.user_requested_scan (
    id integer NOT NULL,
    store_id character varying NOT NULL,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
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
-- Name: click_url_redirect_chains id; Type: DEFAULT; Schema: adtech; Owner: postgres
--

ALTER TABLE ONLY adtech.click_url_redirect_chains ALTER COLUMN id SET DEFAULT nextval('adtech.click_url_redirect_chains_id_seq'::regclass);


--
-- Name: adstxt_crawl_results id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.adstxt_crawl_results ALTER COLUMN id SET DEFAULT nextval('public.adstxt_crawl_results_id_seq'::regclass);


--
-- Name: api_calls id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.api_calls ALTER COLUMN id SET DEFAULT nextval('public.api_calls_id_seq'::regclass);


--
-- Name: app_keyword_rankings id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.app_keyword_rankings ALTER COLUMN id SET DEFAULT nextval('public.app_keyword_rankings_id_seq'::regclass);


--
-- Name: creative_assets id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.creative_assets ALTER COLUMN id SET DEFAULT nextval('public.creative_assets_new_id_seq'::regclass);


--
-- Name: creative_records id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.creative_records ALTER COLUMN id SET DEFAULT nextval('public.creative_records_id_seq1'::regclass);


--
-- Name: description_keywords id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.description_keywords ALTER COLUMN id SET DEFAULT nextval('public.description_keywords_id_seq'::regclass);


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
-- Name: user_requested_scan id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.user_requested_scan ALTER COLUMN id SET DEFAULT nextval('public.user_requested_scan_id_seq'::regclass);


--
-- Name: version_code_api_scan_results id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.version_code_api_scan_results ALTER COLUMN id SET DEFAULT nextval('public.version_code_api_scan_results_id_seq'::regclass);


--
-- Name: version_code_sdk_scan_results id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.version_code_sdk_scan_results ALTER COLUMN id SET DEFAULT nextval('public.version_code_sdk_scan_results_id_seq'::regclass);


--
-- Name: categories categories_pkey; Type: CONSTRAINT; Schema: adtech; Owner: postgres
--

ALTER TABLE ONLY adtech.categories
    ADD CONSTRAINT categories_pkey PRIMARY KEY (id);


--
-- Name: click_url_redirect_chains click_url_redirect_chains_pkey; Type: CONSTRAINT; Schema: adtech; Owner: postgres
--

ALTER TABLE ONLY adtech.click_url_redirect_chains
    ADD CONSTRAINT click_url_redirect_chains_pkey PRIMARY KEY (id);


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
-- Name: ad_domains_old ad_domains_pkey; Type: CONSTRAINT; Schema: public; Owner: james
--

ALTER TABLE ONLY public.ad_domains_old
    ADD CONSTRAINT ad_domains_pkey PRIMARY KEY (id);


--
-- Name: ad_domains_old ad_domains_un; Type: CONSTRAINT; Schema: public; Owner: james
--

ALTER TABLE ONLY public.ad_domains_old
    ADD CONSTRAINT ad_domains_un UNIQUE (domain);


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
-- Name: app_keyword_rankings app_keyword_rankings_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.app_keyword_rankings
    ADD CONSTRAINT app_keyword_rankings_pkey PRIMARY KEY (id);


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
-- Name: description_keywords description_keywords_description_id_keyword_id_key; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.description_keywords
    ADD CONSTRAINT description_keywords_description_id_keyword_id_key UNIQUE (description_id, keyword_id);


--
-- Name: description_keywords description_keywords_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.description_keywords
    ADD CONSTRAINT description_keywords_pkey PRIMARY KEY (id);


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
-- Name: pub_domains_old pub_domains_pkey; Type: CONSTRAINT; Schema: public; Owner: james
--

ALTER TABLE ONLY public.pub_domains_old
    ADD CONSTRAINT pub_domains_pkey PRIMARY KEY (id);


--
-- Name: pub_domains_old pub_domains_un; Type: CONSTRAINT; Schema: public; Owner: james
--

ALTER TABLE ONLY public.pub_domains_old
    ADD CONSTRAINT pub_domains_un UNIQUE (url);


--
-- Name: store_app_z_scores_history store_app_z_scores_history_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.store_app_z_scores_history
    ADD CONSTRAINT store_app_z_scores_history_pkey PRIMARY KEY (target_week, store_id);


--
-- Name: store_apps_country_history store_apps_country_history_pk; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.store_apps_country_history
    ADD CONSTRAINT store_apps_country_history_pk PRIMARY KEY (id);


--
-- Name: store_apps_descriptions store_apps_descriptions_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.store_apps_descriptions
    ADD CONSTRAINT store_apps_descriptions_pkey PRIMARY KEY (id);


--
-- Name: store_apps_descriptions store_apps_descriptions_store_app_language_id_description_d_key; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.store_apps_descriptions
    ADD CONSTRAINT store_apps_descriptions_store_app_language_id_description_d_key UNIQUE (store_app, language_id, description, description_short);


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
-- Name: app_keyword_rankings unique_keyword_ranking; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.app_keyword_rankings
    ADD CONSTRAINT unique_keyword_ranking UNIQUE (crawled_date, country, lang, rank, store_app, keyword);


--
-- Name: user_requested_scan user_requested_scan_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.user_requested_scan
    ADD CONSTRAINT user_requested_scan_pkey PRIMARY KEY (id);


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
-- Name: click_url_redirect_chains_unique; Type: INDEX; Schema: adtech; Owner: postgres
--

CREATE UNIQUE INDEX click_url_redirect_chains_unique ON adtech.click_url_redirect_chains USING btree (run_id, md5(url), md5(redirect_url));


--
-- Name: combined_store_app_companies_idx; Type: INDEX; Schema: adtech; Owner: postgres
--

CREATE UNIQUE INDEX combined_store_app_companies_idx ON adtech.combined_store_apps_companies USING btree (ad_domain, store_app, app_category, company_id);


--
-- Name: idx_combined_store_apps_parent_companies_idx; Type: INDEX; Schema: adtech; Owner: postgres
--

CREATE UNIQUE INDEX idx_combined_store_apps_parent_companies_idx ON adtech.combined_store_apps_parent_companies USING btree (ad_domain, store_app, app_category, company_id);


--
-- Name: idx_store_apps_companies_sdk_new; Type: INDEX; Schema: adtech; Owner: postgres
--

CREATE UNIQUE INDEX idx_store_apps_companies_sdk_new ON adtech.store_apps_companies_sdk USING btree (store_app, company_id, parent_id);


--
-- Name: sdk_packages_package_pattern_idx; Type: INDEX; Schema: adtech; Owner: postgres
--

CREATE INDEX sdk_packages_package_pattern_idx ON adtech.sdk_packages USING btree (package_pattern);


--
-- Name: sdk_path_pattern_idx; Type: INDEX; Schema: adtech; Owner: postgres
--

CREATE INDEX sdk_path_pattern_idx ON adtech.sdk_paths USING btree (path_pattern);


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
-- Name: companies_apps_overview_idx; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE INDEX companies_apps_overview_idx ON frontend.companies_apps_overview USING btree (store_id);


--
-- Name: companies_apps_overview_unique_idx; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE UNIQUE INDEX companies_apps_overview_unique_idx ON frontend.companies_apps_overview USING btree (store_id, company_id, category_slug);


--
-- Name: companies_apps_version_details_count_unique_idx; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE UNIQUE INDEX companies_apps_version_details_count_unique_idx ON frontend.companies_version_details_count USING btree (store, company_name, company_domain, xml_path, value_name);


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
-- Name: companies_category_tag_type_stats_idx; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE UNIQUE INDEX companies_category_tag_type_stats_idx ON frontend.companies_category_tag_type_stats USING btree (store, tag_source, app_category, company_domain, type_url_slug);


--
-- Name: companies_category_tag_type_stats_query_idx; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE INDEX companies_category_tag_type_stats_query_idx ON frontend.companies_category_tag_type_stats USING btree (type_url_slug, app_category);


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
-- Name: keyword_scores_unique; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE UNIQUE INDEX keyword_scores_unique ON frontend.keyword_scores USING btree (store, keyword_id);


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
-- Name: store_apps_overview_unique_idx; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE UNIQUE INDEX store_apps_overview_unique_idx ON frontend.store_apps_overview USING btree (store, store_id);


--
-- Name: store_apps_version_details_store_id_idx; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE INDEX store_apps_version_details_store_id_idx ON frontend.store_apps_version_details USING btree (store_id);


--
-- Name: store_apps_version_details_unique_idx; Type: INDEX; Schema: frontend; Owner: postgres
--

CREATE UNIQUE INDEX store_apps_version_details_unique_idx ON frontend.store_apps_version_details USING btree (version_string_id, store_id, company_id, company_domain, category_slug);


--
-- Name: ix_logging_store_apps_crawl_index; Type: INDEX; Schema: logging; Owner: postgres
--

CREATE INDEX ix_logging_store_apps_crawl_index ON logging.store_apps_crawl USING btree (index);


--
-- Name: logging_store_app_upsert_unique; Type: INDEX; Schema: logging; Owner: postgres
--

CREATE UNIQUE INDEX logging_store_app_upsert_unique ON logging.store_app_waydroid_crawled_at USING btree (store_app, crawl_result, crawled_at);


--
-- Name: store_apps_audit_stamp_idx; Type: INDEX; Schema: logging; Owner: postgres
--

CREATE INDEX store_apps_audit_stamp_idx ON logging.store_apps_audit USING btree (stamp);


--
-- Name: audit_dates_updated_date_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE UNIQUE INDEX audit_dates_updated_date_idx ON public.audit_dates USING btree (updated_date, table_name);


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

CREATE UNIQUE INDEX idx_developer_store_apps_unique ON public.developer_store_apps USING btree (developer_id, store_id);


--
-- Name: idx_ip_geo_ip_created; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_ip_geo_ip_created ON public.ip_geo_snapshots USING btree (ip_address, created_at DESC);


--
-- Name: idx_ip_geo_mitm_uuid; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_ip_geo_mitm_uuid ON public.ip_geo_snapshots USING btree (mitm_uuid);


--
-- Name: idx_my_materialized_view_store_date; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_my_materialized_view_store_date ON public.store_apps_updated_at USING btree (store, date);


--
-- Name: idx_store_apps_created_at; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_store_apps_created_at ON public.store_apps_created_at USING btree (store, date, crawl_source);


--
-- Name: idx_store_apps_created_atx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE UNIQUE INDEX idx_store_apps_created_atx ON public.store_apps_created_at USING btree (store, date, crawl_source);


--
-- Name: idx_store_apps_updated_at; Type: INDEX; Schema: public; Owner: postgres
--

CREATE UNIQUE INDEX idx_store_apps_updated_at ON public.store_apps_updated_at USING btree (store, date);


--
-- Name: idx_top_categories; Type: INDEX; Schema: public; Owner: postgres
--

CREATE UNIQUE INDEX idx_top_categories ON public.top_categories USING btree (store, mapped_category, store_id);


--
-- Name: store_apps_country_history_store_app_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX store_apps_country_history_store_app_idx ON public.store_apps_country_history USING btree (store_app);


--
-- Name: store_apps_country_history_unique_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE UNIQUE INDEX store_apps_country_history_unique_idx ON public.store_apps_country_history USING btree (store_app, country_id, crawled_date);


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
-- Name: ad_domains_old ad_domains_updated_at; Type: TRIGGER; Schema: public; Owner: james
--

CREATE TRIGGER ad_domains_updated_at BEFORE UPDATE ON public.ad_domains_old FOR EACH ROW EXECUTE FUNCTION public.update_modified_column();


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
-- Name: pub_domains_old pub_domains_crawled_at; Type: TRIGGER; Schema: public; Owner: james
--

CREATE TRIGGER pub_domains_crawled_at BEFORE UPDATE OF crawl_result ON public.pub_domains_old FOR EACH ROW EXECUTE FUNCTION public.update_crawled_at();


--
-- Name: pub_domains_old pub_domains_updated_at; Type: TRIGGER; Schema: public; Owner: james
--

CREATE TRIGGER pub_domains_updated_at BEFORE UPDATE ON public.pub_domains_old FOR EACH ROW EXECUTE FUNCTION public.update_modified_column();


--
-- Name: store_apps store_app_audit; Type: TRIGGER; Schema: public; Owner: james
--

CREATE TRIGGER store_app_audit AFTER INSERT OR DELETE OR UPDATE ON public.store_apps FOR EACH ROW EXECUTE FUNCTION public.process_store_app_audit();


--
-- Name: store_apps store_apps_updated_at; Type: TRIGGER; Schema: public; Owner: james
--

CREATE TRIGGER store_apps_updated_at BEFORE UPDATE ON public.store_apps FOR EACH ROW EXECUTE FUNCTION public.update_modified_column();


--
-- Name: version_codes version_codes_updated_at; Type: TRIGGER; Schema: public; Owner: postgres
--

CREATE TRIGGER version_codes_updated_at BEFORE UPDATE ON public.version_codes FOR EACH ROW EXECUTE FUNCTION public.update_modified_column();


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
-- Name: click_url_redirect_chains fk_run_id; Type: FK CONSTRAINT; Schema: adtech; Owner: postgres
--

ALTER TABLE ONLY adtech.click_url_redirect_chains
    ADD CONSTRAINT fk_run_id FOREIGN KEY (run_id) REFERENCES public.version_code_api_scan_results(id) ON DELETE CASCADE;


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
-- Name: store_app_ranks_weekly fk_country; Type: FK CONSTRAINT; Schema: frontend; Owner: postgres
--

ALTER TABLE ONLY frontend.store_app_ranks_weekly
    ADD CONSTRAINT fk_country FOREIGN KEY (country) REFERENCES public.countries(id);


--
-- Name: store_app_ranks_daily fk_country; Type: FK CONSTRAINT; Schema: frontend; Owner: postgres
--

ALTER TABLE ONLY frontend.store_app_ranks_daily
    ADD CONSTRAINT fk_country FOREIGN KEY (country) REFERENCES public.countries(id);


--
-- Name: store_app_ranks_weekly fk_store_app; Type: FK CONSTRAINT; Schema: frontend; Owner: postgres
--

ALTER TABLE ONLY frontend.store_app_ranks_weekly
    ADD CONSTRAINT fk_store_app FOREIGN KEY (store_app) REFERENCES public.store_apps(id);


--
-- Name: store_app_ranks_daily fk_store_app; Type: FK CONSTRAINT; Schema: frontend; Owner: postgres
--

ALTER TABLE ONLY frontend.store_app_ranks_daily
    ADD CONSTRAINT fk_store_app FOREIGN KEY (store_app) REFERENCES public.store_apps(id);


--
-- Name: store_app_ranks_weekly fk_store_category; Type: FK CONSTRAINT; Schema: frontend; Owner: postgres
--

ALTER TABLE ONLY frontend.store_app_ranks_weekly
    ADD CONSTRAINT fk_store_category FOREIGN KEY (store_category) REFERENCES public.store_categories(id);


--
-- Name: store_app_ranks_daily fk_store_category; Type: FK CONSTRAINT; Schema: frontend; Owner: postgres
--

ALTER TABLE ONLY frontend.store_app_ranks_daily
    ADD CONSTRAINT fk_store_category FOREIGN KEY (store_category) REFERENCES public.store_categories(id);


--
-- Name: store_app_ranks_weekly fk_store_collection; Type: FK CONSTRAINT; Schema: frontend; Owner: postgres
--

ALTER TABLE ONLY frontend.store_app_ranks_weekly
    ADD CONSTRAINT fk_store_collection FOREIGN KEY (store_collection) REFERENCES public.store_collections(id);


--
-- Name: store_app_ranks_daily fk_store_collection; Type: FK CONSTRAINT; Schema: frontend; Owner: postgres
--

ALTER TABLE ONLY frontend.store_app_ranks_daily
    ADD CONSTRAINT fk_store_collection FOREIGN KEY (store_collection) REFERENCES public.store_collections(id);


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
-- Name: description_keywords description_keywords_description_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.description_keywords
    ADD CONSTRAINT description_keywords_description_id_fkey FOREIGN KEY (description_id) REFERENCES public.store_apps_descriptions(id) ON DELETE CASCADE;


--
-- Name: description_keywords description_keywords_keyword_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.description_keywords
    ADD CONSTRAINT description_keywords_keyword_id_fkey FOREIGN KEY (keyword_id) REFERENCES public.keywords(id) ON DELETE CASCADE;


--
-- Name: developers developers_fk; Type: FK CONSTRAINT; Schema: public; Owner: james
--

ALTER TABLE ONLY public.developers
    ADD CONSTRAINT developers_fk FOREIGN KEY (store) REFERENCES public.stores(id);


--
-- Name: store_apps_country_history fk_country; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.store_apps_country_history
    ADD CONSTRAINT fk_country FOREIGN KEY (country_id) REFERENCES public.countries(id);


--
-- Name: app_keyword_rankings fk_country; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.app_keyword_rankings
    ADD CONSTRAINT fk_country FOREIGN KEY (country) REFERENCES public.countries(id);


--
-- Name: ip_geo_snapshots fk_country; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.ip_geo_snapshots
    ADD CONSTRAINT fk_country FOREIGN KEY (country_id) REFERENCES public.countries(id);


--
-- Name: app_keyword_rankings fk_language; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.app_keyword_rankings
    ADD CONSTRAINT fk_language FOREIGN KEY (lang) REFERENCES public.languages(id);


--
-- Name: app_keyword_rankings fk_store_app; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.app_keyword_rankings
    ADD CONSTRAINT fk_store_app FOREIGN KEY (store_app) REFERENCES public.store_apps(id);


--
-- Name: app_keyword_rankings fk_store_keyword; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.app_keyword_rankings
    ADD CONSTRAINT fk_store_keyword FOREIGN KEY (keyword) REFERENCES public.keywords(id);


--
-- Name: version_manifests fk_vm_version_code; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.version_manifests
    ADD CONSTRAINT fk_vm_version_code FOREIGN KEY (version_code) REFERENCES public.version_codes(id);


--
-- Name: pub_domains_old pub_domains_fk; Type: FK CONSTRAINT; Schema: public; Owner: james
--

ALTER TABLE ONLY public.pub_domains_old
    ADD CONSTRAINT pub_domains_fk FOREIGN KEY (crawl_result) REFERENCES public.crawl_results(id);


--
-- Name: store_apps_country_history store_apps_country_history_app_fk; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.store_apps_country_history
    ADD CONSTRAINT store_apps_country_history_app_fk FOREIGN KEY (store_app) REFERENCES public.store_apps(id);


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

\unrestrict K1BxpYbyx2EJLL836z6WNdegL3wuQBfU1Mks0njplly6hMOep67dilMhUzaEwna

