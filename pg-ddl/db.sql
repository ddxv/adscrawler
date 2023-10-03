-- DROP SCHEMA public;

CREATE SCHEMA public AUTHORIZATION postgres;

-- DROP SEQUENCE public.ad_domains_id_seq;

CREATE SEQUENCE public.ad_domains_id_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 2147483647
	START 1
	CACHE 1
	NO CYCLE;
-- DROP SEQUENCE public.app_ads_entrys_id_seq;

CREATE SEQUENCE public.app_ads_entrys_id_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 2147483647
	START 1
	CACHE 1
	NO CYCLE;
-- DROP SEQUENCE public.app_ads_map_id_seq;

CREATE SEQUENCE public.app_ads_map_id_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 2147483647
	START 1
	CACHE 1
	NO CYCLE;
-- DROP SEQUENCE public.app_urls_map_id_seq;

CREATE SEQUENCE public.app_urls_map_id_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 2147483647
	START 1
	CACHE 1
	NO CYCLE;
-- DROP SEQUENCE public.crawl_results_id_seq;

CREATE SEQUENCE public.crawl_results_id_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 2147483647
	START 1
	CACHE 1
	NO CYCLE;
-- DROP SEQUENCE public.csv_store_app_dump_id_seq;

CREATE SEQUENCE public.csv_store_app_dump_id_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 2147483647
	START 1
	CACHE 1
	NO CYCLE;
-- DROP SEQUENCE public.developers_id_seq;

CREATE SEQUENCE public.developers_id_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 2147483647
	START 1
	CACHE 1
	NO CYCLE;
-- DROP SEQUENCE public.newtable_id_seq;

CREATE SEQUENCE public.newtable_id_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 2147483647
	START 1
	CACHE 1
	NO CYCLE;
-- DROP SEQUENCE public.pub_domains_id_seq;

CREATE SEQUENCE public.pub_domains_id_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 2147483647
	START 1
	CACHE 1
	NO CYCLE;
-- DROP SEQUENCE public.store_apps_id_seq;

CREATE SEQUENCE public.store_apps_id_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 2147483647
	START 1
	CACHE 1
	NO CYCLE;
-- DROP SEQUENCE public.stores_column1_seq;

CREATE SEQUENCE public.stores_column1_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 2147483647
	START 1
	CACHE 1
	NO CYCLE;-- public.ad_domains definition

-- Drop table

-- DROP TABLE public.ad_domains;

CREATE TABLE public.ad_domains (
	id int4 NOT NULL GENERATED BY DEFAULT AS IDENTITY,
	"domain" varchar NOT NULL,
	created_at timestamp NULL DEFAULT timezone('utc'::text, now()),
	updated_at timestamp NULL DEFAULT timezone('utc'::text, now()),
	CONSTRAINT ad_domains_pkey PRIMARY KEY (id),
	CONSTRAINT ad_domains_un UNIQUE (domain)
);

-- Table Triggers

CREATE TRIGGER ad_domains_updated_at BEFORE
UPDATE
    ON
    public.ad_domains FOR EACH ROW EXECUTE FUNCTION update_modified_column();


-- public.crawl_results definition

-- Drop table

-- DROP TABLE public.crawl_results;

CREATE TABLE public.crawl_results (
	id int4 NOT NULL GENERATED BY DEFAULT AS IDENTITY,
	outcome varchar NULL,
	CONSTRAINT crawl_results_pkey PRIMARY KEY (id)
);


-- public.platforms definition

-- Drop table

-- DROP TABLE public.platforms;

CREATE TABLE public.platforms (
	id int4 NOT NULL DEFAULT nextval('newtable_id_seq'::regclass),
	"name" varchar NOT NULL,
	CONSTRAINT platforms_pk PRIMARY KEY (id),
	CONSTRAINT platforms_un UNIQUE (name)
);


-- public.app_ads_entrys definition

-- Drop table

-- DROP TABLE public.app_ads_entrys;

CREATE TABLE public.app_ads_entrys (
	id int4 NOT NULL GENERATED BY DEFAULT AS IDENTITY,
	ad_domain int4 NOT NULL,
	publisher_id varchar NOT NULL,
	relationship varchar NOT NULL,
	certification_auth varchar NULL,
	notes varchar NULL,
	created_at timestamp NULL DEFAULT timezone('utc'::text, now()),
	updated_at timestamp NULL DEFAULT timezone('utc'::text, now()),
	CONSTRAINT app_ads_entrys_pkey PRIMARY KEY (id),
	CONSTRAINT app_ads_txt_un UNIQUE (ad_domain, publisher_id, relationship),
	CONSTRAINT app_ads_txt_fk FOREIGN KEY (ad_domain) REFERENCES public.ad_domains(id)
);

-- Table Triggers

CREATE TRIGGER app_ads_entrys_updated_at BEFORE
UPDATE
    ON
    public.app_ads_entrys FOR EACH ROW EXECUTE FUNCTION update_modified_column();


-- public.pub_domains definition

-- Drop table

-- DROP TABLE public.pub_domains;

CREATE TABLE public.pub_domains (
	id int4 NOT NULL GENERATED BY DEFAULT AS IDENTITY,
	url varchar NOT NULL,
	created_at timestamp NULL DEFAULT timezone('utc'::text, now()),
	updated_at timestamp NULL DEFAULT timezone('utc'::text, now()),
	crawl_result int4 NULL,
	crawled_at timestamp NULL,
	CONSTRAINT pub_domains_pkey PRIMARY KEY (id),
	CONSTRAINT pub_domains_un UNIQUE (url),
	CONSTRAINT pub_domains_fk FOREIGN KEY (crawl_result) REFERENCES public.crawl_results(id)
);

-- Table Triggers

CREATE TRIGGER pub_domains_crawled_at BEFORE
UPDATE
    OF crawl_result ON
    public.pub_domains FOR EACH ROW EXECUTE FUNCTION update_crawled_at();
CREATE TRIGGER pub_domains_updated_at BEFORE
UPDATE
    ON
    public.pub_domains FOR EACH ROW EXECUTE FUNCTION update_modified_column();


-- public.stores definition

-- Drop table

-- DROP TABLE public.stores;

CREATE TABLE public.stores (
	id int4 NOT NULL DEFAULT nextval('stores_column1_seq'::regclass),
	"name" varchar NOT NULL,
	platform int4 NULL,
	CONSTRAINT stores_pk PRIMARY KEY (id),
	CONSTRAINT stores_un UNIQUE (name),
	CONSTRAINT stores_fk FOREIGN KEY (platform) REFERENCES public.platforms(id)
);


-- public.app_ads_map definition

-- Drop table

-- DROP TABLE public.app_ads_map;

CREATE TABLE public.app_ads_map (
	id int4 NOT NULL GENERATED BY DEFAULT AS IDENTITY,
	pub_domain int4 NOT NULL,
	app_ads_entry int4 NOT NULL,
	created_at timestamp NULL DEFAULT timezone('utc'::text, now()),
	updated_at timestamp NULL DEFAULT timezone('utc'::text, now()),
	CONSTRAINT app_ads_map_pkey PRIMARY KEY (id),
	CONSTRAINT app_ads_map_un UNIQUE (pub_domain, app_ads_entry),
	CONSTRAINT app_ads_map_fk FOREIGN KEY (app_ads_entry) REFERENCES public.app_ads_entrys(id),
	CONSTRAINT app_ads_map_fk_1 FOREIGN KEY (pub_domain) REFERENCES public.pub_domains(id)
);

-- Table Triggers

CREATE TRIGGER app_ads_map_updated_at BEFORE
UPDATE
    ON
    public.app_ads_map FOR EACH ROW EXECUTE FUNCTION update_modified_column();


-- public.app_store_csv_dump definition

-- Drop table

-- DROP TABLE public.app_store_csv_dump;

CREATE TABLE public.app_store_csv_dump (
	id int4 NOT NULL GENERATED BY DEFAULT AS IDENTITY,
	platform int4 NOT NULL,
	store int4 NOT NULL,
	app_name text NULL,
	app_id text NOT NULL,
	category text NULL,
	rating float8 NULL,
	rating_count float8 NULL,
	installs text NULL,
	minimum_installs float8 NULL,
	maximum_installs int8 NULL,
	"free" bool NULL,
	price float8 NULL,
	currency text NULL,
	"size" text NULL,
	minimum_android text NULL,
	developer_id text NULL,
	developer_website text NULL,
	developer_email text NULL,
	released text NULL,
	last_updated text NULL,
	content_rating text NULL,
	privacy_policy text NULL,
	ad_supported bool NULL,
	in_app_purchases bool NULL,
	editors_choice bool NULL,
	CONSTRAINT csv_store_app_dump_pkey PRIMARY KEY (id),
	CONSTRAINT csv_store_app_dump_un UNIQUE (platform, store, app_id),
	CONSTRAINT csv_store_app_dump_fk FOREIGN KEY (platform) REFERENCES public.platforms(id),
	CONSTRAINT csv_store_app_dump_fk_01 FOREIGN KEY (store) REFERENCES public.stores(id)
);


-- public.developers definition

-- Drop table

-- DROP TABLE public.developers;

CREATE TABLE public.developers (
	id int4 NOT NULL GENERATED BY DEFAULT AS IDENTITY,
	store int4 NOT NULL,
	"name" varchar NULL,
	developer_id varchar NOT NULL,
	created_at timestamp NULL DEFAULT timezone('utc'::text, now()),
	updated_at timestamp NULL DEFAULT timezone('utc'::text, now()),
	CONSTRAINT developers_pkey PRIMARY KEY (id),
	CONSTRAINT developers_un UNIQUE (store, developer_id),
	CONSTRAINT developers_fk FOREIGN KEY (store) REFERENCES public.stores(id)
);

-- Table Triggers

CREATE TRIGGER developers_updated_at BEFORE
UPDATE
    ON
    public.developers FOR EACH ROW EXECUTE FUNCTION update_modified_column();


-- public.store_apps definition

-- Drop table

-- DROP TABLE public.store_apps;

CREATE TABLE public.store_apps (
	id int4 NOT NULL GENERATED BY DEFAULT AS IDENTITY,
	developer int4 NULL,
	"name" varchar NULL,
	store_id varchar NOT NULL,
	store int4 NOT NULL,
	category varchar NULL,
	rating float8 NULL,
	review_count float8 NULL,
	installs float8 NULL,
	"free" bool NULL,
	price float8 NULL,
	"size" text NULL,
	minimum_android text NULL,
	developer_email text NULL,
	store_last_updated timestamp NULL,
	content_rating text NULL,
	ad_supported bool NULL,
	in_app_purchases bool NULL,
	editors_choice bool NULL,
	created_at timestamp NULL DEFAULT timezone('utc'::text, now()),
	updated_at timestamp NULL DEFAULT timezone('utc'::text, now()),
	crawl_result int4 NULL,
	CONSTRAINT store_apps_pkey PRIMARY KEY (id),
	CONSTRAINT store_apps_un UNIQUE (store, store_id),
	CONSTRAINT store_apps_fk FOREIGN KEY (store) REFERENCES public.stores(id),
	CONSTRAINT store_apps_fk_03 FOREIGN KEY (crawl_result) REFERENCES public.crawl_results(id),
	CONSTRAINT store_apps_fk_1 FOREIGN KEY (developer) REFERENCES public.developers(id)
);
CREATE INDEX store_apps_updated_at_idx ON public.store_apps USING btree (updated_at);


-- public.store_apps_history definition

--DROP TABLE IF EXISTS store_apps_history;
CREATE TABLE store_apps_history (
    id int4 NOT NULL GENERATED BY DEFAULT AS IDENTITY,
    store_app INT REFERENCES store_apps(id),
    installs float8 NULL,
    review_count float8 NULL,
    rating float8 NULL,
    crawled_date DATE DEFAULT CURRENT_DATE,
    CONSTRAINT store_apps_history_pkey PRIMARY KEY (id),
    CONSTRAINT store_apps_history_un UNIQUE (store_app, crawled_date),
    CONSTRAINT store_apps_history_fk FOREIGN KEY (store_app) REFERENCES public.store_apps(id)
);

-- Table Triggers

CREATE TRIGGER store_app_audit AFTER
INSERT
    OR
DELETE
    OR
UPDATE
    ON
    public.store_apps FOR EACH ROW EXECUTE FUNCTION process_store_app_audit();
CREATE TRIGGER store_apps_updated_at BEFORE
UPDATE
    ON
    public.store_apps FOR EACH ROW EXECUTE FUNCTION update_modified_column();


-- public.app_urls_map definition

-- Drop table

-- DROP TABLE public.app_urls_map;

CREATE TABLE public.app_urls_map (
	id int4 NOT NULL GENERATED BY DEFAULT AS IDENTITY,
	store_app int4 NOT NULL,
	pub_domain int4 NOT NULL,
	created_at timestamp NULL DEFAULT timezone('utc'::text, now()),
	updated_at timestamp NULL DEFAULT timezone('utc'::text, now()),
	CONSTRAINT app_urls_map_pkey PRIMARY KEY (id),
	CONSTRAINT app_urls_un UNIQUE (store_app),
	CONSTRAINT app_urls_fk FOREIGN KEY (store_app) REFERENCES public.store_apps(id),
	CONSTRAINT app_urls_fk_1 FOREIGN KEY (pub_domain) REFERENCES public.pub_domains(id)
);

-- Table Triggers

CREATE TRIGGER app_urls_map_updated_at BEFORE
UPDATE
    ON
    public.app_urls_map FOR EACH ROW EXECUTE FUNCTION update_modified_column();


-- public.app_ads_view source

CREATE MATERIALIZED VIEW public.app_ads_view
TABLESPACE pg_default
AS SELECT DISTINCT pd.url AS developer_domain_url,
    aae.ad_domain,
    aae.publisher_id,
    ad.domain AS ad_domain_url,
    aae.relationship,
    pd.crawl_result,
    pd.crawled_at AS developer_domain_crawled_at,
    ad.updated_at AS ad_domain_updated_at,
    aam.updated_at AS txt_entry_crawled_at
   FROM app_ads_entrys aae
     LEFT JOIN ad_domains ad ON ad.id = aae.ad_domain
     LEFT JOIN app_ads_map aam ON aam.app_ads_entry = aae.id
     LEFT JOIN pub_domains pd ON pd.id = aam.pub_domain
  WHERE pd.crawled_at::date = aam.updated_at::date AND pd.crawl_result = 1
WITH DATA;

-- View indexes:
CREATE UNIQUE INDEX app_ads_view_developer_domain_url_idx ON public.app_ads_view USING btree (developer_domain_url, ad_domain, publisher_id, ad_domain_url, relationship);


-- public.audit_dates source

CREATE MATERIALIZED VIEW public.audit_dates
TABLESPACE pg_default
AS WITH sa AS (
         SELECT store_apps_audit.stamp::date AS updated_date,
            'store_apps'::text AS table_name,
            count(1) AS updated_count
           FROM logging.store_apps_audit
          GROUP BY (store_apps_audit.stamp::date)
        )
 SELECT sa.updated_date,
    sa.table_name,
    sa.updated_count
   FROM sa
WITH DATA;

-- View indexes:
CREATE UNIQUE INDEX audit_dates_updated_date_idx ON public.audit_dates USING btree (updated_date, table_name);


-- public.metric_totals source

CREATE MATERIALIZED VIEW public.metric_totals
TABLESPACE pg_default
AS SELECT pdwi.store,
    pdwi.category,
    count(DISTINCT pdwi.url) AS publisher_urls,
    sum(pdwi.app_count) AS sc_app_count,
    sum(pdwi.total_installs) AS sc_installs,
    sum(pdwi.total_review) AS sc_reviews
   FROM publisher_domain_with_installs pdwi
  WHERE (pdwi.url::text IN ( SELECT DISTINCT pnv.publisher_domain_url
           FROM publisher_network_view pnv))
  GROUP BY pdwi.store, pdwi.category
WITH DATA;

-- View indexes:
CREATE UNIQUE INDEX metric_totals_idx ON public.metric_totals USING btree (store, category);


-- public.network_counts source

CREATE MATERIALIZED VIEW public.network_counts
TABLESPACE pg_default
AS WITH const AS (
         SELECT count(DISTINCT publisher_network_view.publisher_domain_url) AS count
           FROM publisher_network_view
        ), grouped AS (
         SELECT publisher_network_view.ad_domain_url,
            publisher_network_view.relationship,
            count(*) AS publishers_count
           FROM publisher_network_view
          GROUP BY publisher_network_view.ad_domain_url, publisher_network_view.relationship
        )
 SELECT grouped.ad_domain_url,
    grouped.relationship,
    grouped.publishers_count,
    const.count AS publishers_total
   FROM grouped
     CROSS JOIN const
  ORDER BY grouped.publishers_count DESC
WITH DATA;

-- View indexes:
CREATE UNIQUE INDEX network_counts_view_idx ON public.network_counts USING btree (ad_domain_url, relationship);


-- public.network_counts_top source

CREATE MATERIALIZED VIEW public.network_counts_top
TABLESPACE pg_default
AS WITH pubids AS (
         SELECT DISTINCT pdwi.url
           FROM publisher_domain_with_installs pdwi
          WHERE pdwi.total_installs > 10000000::double precision OR pdwi.store = 2 AND pdwi.total_review > 10000::double precision
        ), const AS (
         SELECT count(DISTINCT publisher_network_view.publisher_domain_url) AS count
           FROM publisher_network_view
          WHERE (publisher_network_view.publisher_domain_url::text IN ( SELECT pubids.url
                   FROM pubids))
        ), grouped AS (
         SELECT publisher_network_view.ad_domain_url,
            publisher_network_view.relationship,
            count(*) AS publishers_count
           FROM publisher_network_view
          WHERE (publisher_network_view.publisher_domain_url::text IN ( SELECT pubids.url
                   FROM pubids))
          GROUP BY publisher_network_view.ad_domain_url, publisher_network_view.relationship
        )
 SELECT grouped.ad_domain_url,
    grouped.relationship,
    grouped.publishers_count,
    const.count AS publishers_total
   FROM grouped
     CROSS JOIN const
  ORDER BY grouped.publishers_count DESC
WITH DATA;

-- View indexes:
CREATE UNIQUE INDEX network_counts_top_view_idx ON public.network_counts_top USING btree (ad_domain_url, relationship);


-- public.networks_with_app_metrics source

CREATE MATERIALIZED VIEW public.networks_with_app_metrics
TABLESPACE pg_default
AS WITH tot AS (
         SELECT pnv.relationship,
            pdwi.store,
            pdwi.category,
            count(DISTINCT pnv.publisher_domain_url) AS total_publisher_urls,
            sum(pdwi.app_count) AS total_apps,
            sum(pdwi.total_installs) AS total_installs,
            sum(pdwi.total_review) AS total_reviews
           FROM publisher_domain_with_installs pdwi
             JOIN ( SELECT DISTINCT publisher_network_view.publisher_domain_url,
                    publisher_network_view.relationship
                   FROM publisher_network_view) pnv ON pdwi.url::text = pnv.publisher_domain_url::text
          WHERE (pdwi.url::text IN ( SELECT publisher_network_view.publisher_domain_url
                   FROM publisher_network_view))
          GROUP BY pnv.relationship, pdwi.store, pdwi.category
        ), nwm AS (
         SELECT pnv.ad_domain_url,
            pnv.relationship,
            pdwi.store,
            pdwi.category,
            count(DISTINCT pnv.publisher_domain_url) AS publisher_urls,
            sum(pdwi.app_count) AS apps,
            sum(pdwi.total_review) AS reviews,
            sum(pdwi.total_installs) AS installs
           FROM publisher_network_view pnv
             JOIN publisher_domain_with_installs pdwi ON pnv.publisher_domain_url::text = pdwi.url::text
          GROUP BY CUBE(pnv.ad_domain_url, pnv.relationship, pdwi.store, pdwi.category)
        )
 SELECT nwm.ad_domain_url,
    nwm.relationship,
    nwm.store,
    nwm.category,
    nwm.publisher_urls,
    tot.total_publisher_urls,
    nwm.installs,
    tot.total_installs,
    nwm.apps,
    tot.total_apps,
    nwm.reviews,
    tot.total_reviews
   FROM nwm
     LEFT JOIN tot ON nwm.store = tot.store AND nwm.category = tot.category AND nwm.relationship::text = tot.relationship::text
  WHERE nwm.relationship IS NOT NULL AND nwm.ad_domain_url IS NOT NULL AND nwm.category IS NOT NULL AND nwm.store IS NOT NULL
  ORDER BY tot.total_publisher_urls DESC NULLS LAST, nwm.publisher_urls DESC NULLS LAST
WITH DATA;

-- View indexes:
CREATE UNIQUE INDEX networks_with_app_metrics_idx ON public.networks_with_app_metrics USING btree (ad_domain_url, relationship, store, category);


-- public.pg_stat_statements source

CREATE OR REPLACE VIEW public.pg_stat_statements
AS SELECT pg_stat_statements.userid,
    pg_stat_statements.dbid,
    pg_stat_statements.toplevel,
    pg_stat_statements.queryid,
    pg_stat_statements.query,
    pg_stat_statements.plans,
    pg_stat_statements.total_plan_time,
    pg_stat_statements.min_plan_time,
    pg_stat_statements.max_plan_time,
    pg_stat_statements.mean_plan_time,
    pg_stat_statements.stddev_plan_time,
    pg_stat_statements.calls,
    pg_stat_statements.total_exec_time,
    pg_stat_statements.min_exec_time,
    pg_stat_statements.max_exec_time,
    pg_stat_statements.mean_exec_time,
    pg_stat_statements.stddev_exec_time,
    pg_stat_statements.rows,
    pg_stat_statements.shared_blks_hit,
    pg_stat_statements.shared_blks_read,
    pg_stat_statements.shared_blks_dirtied,
    pg_stat_statements.shared_blks_written,
    pg_stat_statements.local_blks_hit,
    pg_stat_statements.local_blks_read,
    pg_stat_statements.local_blks_dirtied,
    pg_stat_statements.local_blks_written,
    pg_stat_statements.temp_blks_read,
    pg_stat_statements.temp_blks_written,
    pg_stat_statements.blk_read_time,
    pg_stat_statements.blk_write_time,
    pg_stat_statements.temp_blk_read_time,
    pg_stat_statements.temp_blk_write_time,
    pg_stat_statements.wal_records,
    pg_stat_statements.wal_fpi,
    pg_stat_statements.wal_bytes,
    pg_stat_statements.jit_functions,
    pg_stat_statements.jit_generation_time,
    pg_stat_statements.jit_inlining_count,
    pg_stat_statements.jit_inlining_time,
    pg_stat_statements.jit_optimization_count,
    pg_stat_statements.jit_optimization_time,
    pg_stat_statements.jit_emission_count,
    pg_stat_statements.jit_emission_time
   FROM pg_stat_statements(true) pg_stat_statements(userid, dbid, toplevel, queryid, query, plans, total_plan_time, min_plan_time, max_plan_time, mean_plan_time, stddev_plan_time, calls, total_exec_time, min_exec_time, max_exec_time, mean_exec_time, stddev_exec_time, rows, shared_blks_hit, shared_blks_read, shared_blks_dirtied, shared_blks_written, local_blks_hit, local_blks_read, local_blks_dirtied, local_blks_written, temp_blks_read, temp_blks_written, blk_read_time, blk_write_time, temp_blk_read_time, temp_blk_write_time, wal_records, wal_fpi, wal_bytes, jit_functions, jit_generation_time, jit_inlining_count, jit_inlining_time, jit_optimization_count, jit_optimization_time, jit_emission_count, jit_emission_time);


-- public.pg_stat_statements_info source

CREATE OR REPLACE VIEW public.pg_stat_statements_info
AS SELECT pg_stat_statements_info.dealloc,
    pg_stat_statements_info.stats_reset
   FROM pg_stat_statements_info() pg_stat_statements_info(dealloc, stats_reset);


-- public.publisher_domain_with_installs source

CREATE MATERIALIZED VIEW public.publisher_domain_with_installs
TABLESPACE pg_default
AS SELECT pd.id AS pub_domain_id,
    pd.url,
    sa.store,
        CASE
            WHEN regexp_replace(lower(sa.category::text), ' & '::text, '_and_'::text) = ANY (ARRAY['action'::text, 'casual'::text, 'adventure'::text, 'arcade'::text, 'board'::text, 'card'::text, 'casino'::text, 'puzzle'::text, 'racing'::text, 'simulation'::text, 'strategy'::text, 'trivia'::text, 'word'::text]) THEN 'game_'::text || regexp_replace(lower(sa.category::text), ' & '::text, '_and_'::text)
            WHEN regexp_replace(lower(sa.category::text), ' & '::text, '_and_'::text) = 'news'::text THEN 'magazines_and_newspapers'::text
            WHEN regexp_replace(lower(sa.category::text), ' & '::text, '_and_'::text) = 'educational'::text THEN 'education'::text
            WHEN regexp_replace(lower(sa.category::text), ' & '::text, '_and_'::text) = 'book'::text THEN 'books_and_reference'::text
            WHEN regexp_replace(lower(sa.category::text), ' & '::text, '_and_'::text) = 'navigation'::text THEN 'maps_and_navigation'::text
            WHEN regexp_replace(lower(sa.category::text), ' & '::text, '_and_'::text) = 'music'::text THEN 'music_and_audio'::text
            WHEN regexp_replace(lower(sa.category::text), ' & '::text, '_and_'::text) = 'photography'::text THEN 'photo_and_video'::text
            WHEN regexp_replace(lower(sa.category::text), ' & '::text, '_and_'::text) = 'reference'::text THEN 'books_and_reference'::text
            WHEN regexp_replace(lower(sa.category::text), ' & '::text, '_and_'::text) = 'role playing'::text THEN 'game_role_playing'::text
            WHEN regexp_replace(lower(sa.category::text), ' & '::text, '_and_'::text) = 'social'::text THEN 'social networking'::text
            WHEN regexp_replace(lower(sa.category::text), ' & '::text, '_and_'::text) = 'travel'::text THEN 'travel_and_local'::text
            WHEN regexp_replace(lower(sa.category::text), ' & '::text, '_and_'::text) = 'utilities'::text THEN 'tools'::text
            WHEN regexp_replace(lower(sa.category::text), ' & '::text, '_and_'::text) = 'video players_and_editors'::text THEN 'video_players'::text
            ELSE regexp_replace(lower(sa.category::text), ' & '::text, '_and_'::text)
        END AS category,
    count(*) AS app_count,
    sum(COALESCE(NULLIF(sa.installs, 'NaN'::double precision), 0::double precision)) AS total_installs,
    sum(COALESCE(NULLIF(sa.review_count, 'NaN'::double precision), 0::double precision)) AS total_review
   FROM pub_domains pd
     LEFT JOIN app_urls_map aum ON aum.pub_domain = pd.id
     LEFT JOIN store_apps sa ON aum.store_app = sa.id
  WHERE sa.crawl_result = 1
  GROUP BY pd.id, pd.url, sa.store, (
        CASE
            WHEN regexp_replace(lower(sa.category::text), ' & '::text, '_and_'::text) = ANY (ARRAY['action'::text, 'casual'::text, 'adventure'::text, 'arcade'::text, 'board'::text, 'card'::text, 'casino'::text, 'puzzle'::text, 'racing'::text, 'simulation'::text, 'strategy'::text, 'trivia'::text, 'word'::text]) THEN 'game_'::text || regexp_replace(lower(sa.category::text), ' & '::text, '_and_'::text)
            WHEN regexp_replace(lower(sa.category::text), ' & '::text, '_and_'::text) = 'news'::text THEN 'magazines_and_newspapers'::text
            WHEN regexp_replace(lower(sa.category::text), ' & '::text, '_and_'::text) = 'educational'::text THEN 'education'::text
            WHEN regexp_replace(lower(sa.category::text), ' & '::text, '_and_'::text) = 'book'::text THEN 'books_and_reference'::text
            WHEN regexp_replace(lower(sa.category::text), ' & '::text, '_and_'::text) = 'navigation'::text THEN 'maps_and_navigation'::text
            WHEN regexp_replace(lower(sa.category::text), ' & '::text, '_and_'::text) = 'music'::text THEN 'music_and_audio'::text
            WHEN regexp_replace(lower(sa.category::text), ' & '::text, '_and_'::text) = 'photography'::text THEN 'photo_and_video'::text
            WHEN regexp_replace(lower(sa.category::text), ' & '::text, '_and_'::text) = 'reference'::text THEN 'books_and_reference'::text
            WHEN regexp_replace(lower(sa.category::text), ' & '::text, '_and_'::text) = 'role playing'::text THEN 'game_role_playing'::text
            WHEN regexp_replace(lower(sa.category::text), ' & '::text, '_and_'::text) = 'social'::text THEN 'social networking'::text
            WHEN regexp_replace(lower(sa.category::text), ' & '::text, '_and_'::text) = 'travel'::text THEN 'travel_and_local'::text
            WHEN regexp_replace(lower(sa.category::text), ' & '::text, '_and_'::text) = 'utilities'::text THEN 'tools'::text
            WHEN regexp_replace(lower(sa.category::text), ' & '::text, '_and_'::text) = 'video players_and_editors'::text THEN 'video_players'::text
            ELSE regexp_replace(lower(sa.category::text), ' & '::text, '_and_'::text)
        END)
  ORDER BY (sum(COALESCE(NULLIF(sa.installs, 'NaN'::double precision), 0::double precision))) DESC
WITH DATA;

-- View indexes:
CREATE UNIQUE INDEX publisher_domain_with_installs_unique_indx ON public.publisher_domain_with_installs USING btree (pub_domain_id, url, store, category);


-- public.publisher_network_view source

CREATE MATERIALIZED VIEW public.publisher_network_view
TABLESPACE pg_default
AS SELECT DISTINCT av.developer_domain_url AS publisher_domain_url,
    av.ad_domain_url,
    av.relationship
   FROM app_ads_view av
  WHERE av.developer_domain_crawled_at::date = av.txt_entry_crawled_at::date
WITH DATA;

-- View indexes:
CREATE UNIQUE INDEX publisher_network_view_uniq_idx ON public.publisher_network_view USING btree (publisher_domain_url, ad_domain_url, relationship);


-- public.publisher_url_developer_ids_uniques source

CREATE MATERIALIZED VIEW public.publisher_url_developer_ids_uniques
TABLESPACE pg_default
AS WITH uniq_pub_urls AS (
         SELECT app_ads_view.ad_domain_url,
            app_ads_view.publisher_id,
            count(DISTINCT app_ads_view.developer_domain_url) AS unique_publisher_urls
           FROM app_ads_view
          WHERE app_ads_view.relationship::text = 'DIRECT'::text
          GROUP BY app_ads_view.ad_domain_url, app_ads_view.publisher_id
        ), dev_url_map AS (
         SELECT DISTINCT d.developer_id,
            pd.url AS pub_domain_url
           FROM store_apps sa
             LEFT JOIN developers d ON d.id = sa.developer
             LEFT JOIN app_urls_map aum ON aum.store_app = sa.id
             LEFT JOIN pub_domains pd ON pd.id = aum.pub_domain
          WHERE sa.crawl_result = 1 AND pd.crawl_result = 1
        ), uniq_dev_ids AS (
         SELECT aav.ad_domain_url,
            aav.publisher_id,
            count(DISTINCT dum.developer_id) AS unique_developer_ids
           FROM app_ads_view aav
             LEFT JOIN dev_url_map dum ON dum.pub_domain_url::text = aav.developer_domain_url::text
          WHERE aav.relationship::text = 'DIRECT'::text
          GROUP BY aav.ad_domain_url, aav.publisher_id
        )
 SELECT upu.ad_domain_url,
    upu.publisher_id,
    upu.unique_publisher_urls,
    udi.unique_developer_ids,
        CASE
            WHEN upu.unique_publisher_urls <= 1 OR udi.unique_developer_ids <= 1 THEN 1
            ELSE 0
        END AS is_unique
   FROM uniq_pub_urls upu
     FULL JOIN uniq_dev_ids udi ON upu.ad_domain_url::text = udi.ad_domain_url::text AND upu.publisher_id::text = udi.publisher_id::text
WITH DATA;


--Make index of just categories
CREATE MATERIALIZED VIEW mv_app_categories AS
WITH CategoryMapping AS (
    SELECT
        original_category,
        CASE
            WHEN original_category IN ('action', 'casual', 'adventure', 'arcade', 'board', 'card', 'casino', 'puzzle', 'racing', 'simulation' , 'strategy', 'trivia', 'word') THEN 'game_' || original_category
            WHEN original_category = 'news' THEN 'magazines_and_newspapers'
            WHEN original_category = 'educational' THEN 'education'
            WHEN original_category = 'book' THEN 'books_and_reference'
            WHEN original_category = 'navigation' THEN 'maps_and_navigation'
            WHEN original_category = 'music' THEN 'music_and_audio'
            WHEN original_category = 'photography' THEN 'photo_and_video'
            WHEN original_category = 'reference' THEN 'books_and_reference'
            WHEN original_category = 'role playing' THEN 'game_role_playing'
            WHEN original_category = 'social' THEN 'social networking'
            WHEN original_category = 'travel' THEN 'travel_and_local'
            WHEN original_category = 'utilities' THEN 'tools'
            WHEN original_category = 'video players_and_editors' THEN 'video_players'
            ELSE original_category
        END AS mapped_category
    FROM (
        SELECT DISTINCT regexp_replace(lower(sa.category), ' & ', '_and_') AS original_category
        FROM store_apps sa
    ) AS DistinctCategories
)
SELECT
    sa.store,
    cm.mapped_category AS category,
    COUNT(*) AS app_count
FROM store_apps sa
JOIN CategoryMapping cm ON regexp_replace(lower(sa.category), ' & ', '_and_') = cm.original_category
GROUP BY sa.store, cm.mapped_category
ORDER BY sa.store, cm.mapped_category
;



CREATE OR REPLACE FUNCTION public.pg_stat_statements(showtext boolean, OUT userid oid, OUT dbid oid, OUT toplevel boolean, OUT queryid bigint, OUT query text, OUT plans bigint, OUT total_plan_time double precision, OUT min_plan_time double precision, OUT max_plan_time double precision, OUT mean_plan_time double precision, OUT stddev_plan_time double precision, OUT calls bigint, OUT total_exec_time double precision, OUT min_exec_time double precision, OUT max_exec_time double precision, OUT mean_exec_time double precision, OUT stddev_exec_time double precision, OUT rows bigint, OUT shared_blks_hit bigint, OUT shared_blks_read bigint, OUT shared_blks_dirtied bigint, OUT shared_blks_written bigint, OUT local_blks_hit bigint, OUT local_blks_read bigint, OUT local_blks_dirtied bigint, OUT local_blks_written bigint, OUT temp_blks_read bigint, OUT temp_blks_written bigint, OUT blk_read_time double precision, OUT blk_write_time double precision, OUT temp_blk_read_time double precision, OUT temp_blk_write_time double precision, OUT wal_records bigint, OUT wal_fpi bigint, OUT wal_bytes numeric, OUT jit_functions bigint, OUT jit_generation_time double precision, OUT jit_inlining_count bigint, OUT jit_inlining_time double precision, OUT jit_optimization_count bigint, OUT jit_optimization_time double precision, OUT jit_emission_count bigint, OUT jit_emission_time double precision)
 RETURNS SETOF record
 LANGUAGE c
 PARALLEL SAFE STRICT
AS '$libdir/pg_stat_statements', $function$pg_stat_statements_1_10$function$
;

CREATE OR REPLACE FUNCTION public.pg_stat_statements_info(OUT dealloc bigint, OUT stats_reset timestamp with time zone)
 RETURNS record
 LANGUAGE c
 PARALLEL SAFE STRICT
AS '$libdir/pg_stat_statements', $function$pg_stat_statements_info$function$
;

CREATE OR REPLACE FUNCTION public.pg_stat_statements_reset(userid oid DEFAULT 0, dbid oid DEFAULT 0, queryid bigint DEFAULT 0)
 RETURNS void
 LANGUAGE c
 PARALLEL SAFE STRICT
AS '$libdir/pg_stat_statements', $function$pg_stat_statements_reset_1_7$function$
;

CREATE OR REPLACE FUNCTION public.process_store_app_audit()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
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

$function$
;

CREATE OR REPLACE FUNCTION public.snapshot_apps()
 RETURNS void
 LANGUAGE plpgsql
AS $function$
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

$function$
;

CREATE OR REPLACE FUNCTION public.snapshot_pub_domains()
 RETURNS void
 LANGUAGE plpgsql
AS $function$
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

$function$
;

CREATE OR REPLACE FUNCTION public.snapshot_store_apps()
 RETURNS void
 LANGUAGE plpgsql
AS $function$
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

$function$
;

CREATE OR REPLACE FUNCTION public.update_crawled_at()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$ BEGIN NEW.crawled_at = now();
RETURN NEW;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.update_modified_column()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$ BEGIN NEW.updated_at = now();

RETURN NEW;

END;

$function$
;
