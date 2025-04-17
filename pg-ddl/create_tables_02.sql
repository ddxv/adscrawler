CREATE TABLE public.developers (
    id int4 NOT NULL GENERATED BY DEFAULT AS IDENTITY,
    store smallint NOT NULL,
    name varchar NULL,
    developer_id varchar NOT NULL,
    created_at timestamp NULL DEFAULT timezone('utc'::text, now()),
    updated_at timestamp NULL DEFAULT timezone('utc'::text, now()),
    CONSTRAINT developers_pkey PRIMARY KEY (id),
    CONSTRAINT developers_un UNIQUE (store, developer_id),
    CONSTRAINT developers_fk FOREIGN KEY (store) REFERENCES public.stores (id)
);
CREATE INDEX developers_name_idx ON developers USING gin (
    to_tsvector('simple', name)
);
CREATE INDEX developers_developer_id_idx ON public.developers (developer_id);


CREATE TABLE store_collections (
    id smallint NOT NULL GENERATED BY DEFAULT AS IDENTITY,
    store int4 NOT NULL,
    collection varchar NOT NULL,
    CONSTRAINT store_collections_pk PRIMARY KEY (id),
    CONSTRAINT store_collections_fk FOREIGN KEY (
        store
    ) REFERENCES public.stores (id),
    CONSTRAINT store_collections_un UNIQUE (
        store,
        collection
    )
);


CREATE TABLE store_categories (
    id smallint NOT NULL GENERATED BY DEFAULT AS IDENTITY,
    store int4 NOT NULL,
    category varchar NOT NULL,
    CONSTRAINT store_categories_pk PRIMARY KEY (id),
    CONSTRAINT store_categories_fk FOREIGN KEY (
        store
    ) REFERENCES public.stores (id),
    CONSTRAINT store_categories_un UNIQUE (
        store,
        category
    )
);

CREATE TABLE app_rankings (
    id serial PRIMARY KEY,
    crawled_date date NOT NULL,
    country smallint NOT NULL,
    store_collection smallint NOT NULL,
    store_category smallint NOT NULL,
    rank smallint NOT NULL,
    store_app int NOT NULL,
    CONSTRAINT fk_store_collection FOREIGN KEY (
        store_collection
    ) REFERENCES store_collections (id),
    CONSTRAINT fk_store_category FOREIGN KEY (
        store_category
    ) REFERENCES store_categories (id),
    CONSTRAINT fk_country FOREIGN KEY (country) REFERENCES countries (id),
    CONSTRAINT fk_store_app FOREIGN KEY (store_app) REFERENCES store_apps (id),
    CONSTRAINT unique_ranking UNIQUE (
        crawled_date, country,
        rank,
        store_collection,
        store_category
    )
);


-- DROP TABLE public.store_apps;
CREATE TABLE store_apps (
    id int4 NOT NULL GENERATED BY DEFAULT AS IDENTITY,
    developer int4 NULL,
    name varchar NULL,
    store_id varchar NOT NULL,
    store int4 NOT NULL,
    category varchar NULL,
    rating float8 NULL,
    rating_count int NULL,
    review_count float8 NULL,
    installs float8 NULL,
    free bool NULL,
    price float8 NULL,
    size text NULL,
    minimum_android text NULL,
    developer_email text NULL,
    store_last_updated timestamp NULL,
    content_rating text NULL,
    ad_supported bool NULL,
    in_app_purchases bool NULL,
    editors_choice bool NULL,
    icon_url_512 varchar NULL,
    featured_image_url varchar NULL,
    phone_image_url_1 varchar NULL,
    phone_image_url_2 varchar NULL,
    phone_image_url_3 varchar NULL,
    tablet_image_url_1 varchar NULL,
    tablet_image_url_2 varchar NULL,
    tablet_image_url_3 varchar NULL,
    release_date date NULL,
    created_at timestamp NULL DEFAULT timezone('utc'::text, now()),
    updated_at timestamp NULL DEFAULT timezone('utc'::text, now()),
    crawl_result int4 NULL,
    CONSTRAINT store_apps_pkey PRIMARY KEY (id),
    CONSTRAINT store_apps_un UNIQUE (store, store_id),
    CONSTRAINT store_apps_fk FOREIGN KEY (store) REFERENCES public.stores (id),
    CONSTRAINT store_apps_fk_03 FOREIGN KEY (
        crawl_result
    ) REFERENCES public.crawl_results (id),
    CONSTRAINT store_apps_fk_1 FOREIGN KEY (
        developer
    ) REFERENCES public.developers (id)
);

CREATE INDEX store_apps_updated_at_idx ON public.store_apps USING btree (
    updated_at
);
CREATE INDEX store_apps_store_id_idx ON public.store_apps USING btree (
    store_id
);
CREATE INDEX store_apps_name_idx ON store_apps USING gin (
    to_tsvector('simple', name)
);
CREATE INDEX store_apps_developer_idx ON public.store_apps USING btree (
    developer
);




-- Table Triggers
CREATE TRIGGER developers_updated_at BEFORE
UPDATE
ON
public.developers FOR EACH ROW EXECUTE FUNCTION update_modified_column();





-- public.store_apps_country_history definition

CREATE TABLE public.store_apps_country_history (
    store_app int4 NOT NULL,
    review_count float8 NULL,
    rating float8 NULL,
    crawled_date date NULL,
    rating_count int4 NULL,
    histogram _int8 DEFAULT ARRAY[]::integer [] NULL,
    installs int8 NULL,
    id int4 GENERATED BY DEFAULT AS IDENTITY (
        INCREMENT BY 1 MINVALUE 1 MAXVALUE 2147483647 START 1 CACHE 1 NO CYCLE
    ) NOT NULL,
    country_id int2 NULL,
    CONSTRAINT store_apps_country_history_pk PRIMARY KEY (id)
);


-- public.store_apps_country_history foreign keys

ALTER TABLE public.store_apps_country_history ADD CONSTRAINT fk_country FOREIGN KEY (
    country_id
) REFERENCES public.countries (id);
ALTER TABLE public.store_apps_country_history ADD CONSTRAINT store_apps_country_history_app_fk FOREIGN KEY (
    store_app
) REFERENCES public.store_apps (id);
CREATE INDEX store_apps_country_history_store_app_idx ON public.store_apps_country_history (
    store_app
);



CREATE TABLE logging.developers_crawled_at (
    developer int4 NOT NULL,
    apps_crawled_at timestamp NULL,
    CONSTRAINT developers_crawled_at_pk PRIMARY KEY (developer),
    CONSTRAINT newtable_fk FOREIGN KEY (
        developer
    ) REFERENCES public.developers (id)
);


CREATE TABLE logging.store_app_sources (
    store int2 NOT NULL,
    store_app int4 NOT NULL,
    crawl_source text NULL,
    CONSTRAINT store_app_sources_pk PRIMARY KEY (store, store_app),
    CONSTRAINT store_app_sources_store_fk FOREIGN KEY (
        store
    ) REFERENCES public.stores (id),
    CONSTRAINT store_app_sources_app_fk FOREIGN KEY (
        store_app
    ) REFERENCES public.store_apps (id) ON DELETE CASCADE
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
    CONSTRAINT app_urls_fk FOREIGN KEY (
        store_app
    ) REFERENCES public.store_apps (id),
    CONSTRAINT app_urls_fk_1 FOREIGN KEY (
        pub_domain
    ) REFERENCES public.pub_domains (id)
);

-- Table Triggers

CREATE TRIGGER app_urls_map_updated_at BEFORE
UPDATE
ON
public.app_urls_map FOR EACH ROW EXECUTE FUNCTION update_modified_column();


CREATE TABLE version_codes (
    id int4 NOT NULL GENERATED BY DEFAULT AS IDENTITY,
    store_app integer NOT NULL,
    version_code integer NOT NULL,
    crawl_result smallint NOT NULL,
    updated_at timestamp NULL DEFAULT timezone('utc'::text, now()),
    CONSTRAINT version_codes_pkey PRIMARY KEY (id),
    CONSTRAINT version_codes_un UNIQUE (store_app, version_code),
    CONSTRAINT vc_fk_store_app FOREIGN KEY (store_app) REFERENCES store_apps (
        id
    )
);


CREATE TRIGGER version_codes_updated_at BEFORE
UPDATE
ON
public.version_codes FOR EACH ROW EXECUTE FUNCTION update_modified_column();

-- CREATE TABLE version_details (
--     id int4 NOT NULL GENERATED BY DEFAULT AS IDENTITY,
--     version_code integer NOT NULL,
--     xml_path text NOT NULL,
--     tag text,
--     value_name text,
--     CONSTRAINT version_details_pkey PRIMARY KEY (id),
--     CONSTRAINT fk_vd_version_code FOREIGN KEY (
--         version_code
--     ) REFERENCES version_codes (id)
-- );

-- CREATE EXTENSION IF NOT EXISTS pg_trgm;
-- CREATE INDEX idx_value_name_trgm ON version_details USING gin (
--     value_name gin_trgm_ops
-- );
