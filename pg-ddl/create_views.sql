CREATE MATERIALIZED VIEW public.app_ads_view
TABLESPACE pg_default
AS SELECT DISTINCT
    pd.url AS developer_domain_url,
    aae.ad_domain,
    aae.publisher_id,
    ad.domain AS ad_domain_url,
    aae.relationship,
    pd.crawl_result,
    pd.crawled_at AS developer_domain_crawled_at,
    ad.updated_at AS ad_domain_updated_at,
    aam.updated_at AS txt_entry_crawled_at
FROM app_ads_entrys aae
LEFT JOIN ad_domains ad ON aae.ad_domain = ad.id
LEFT JOIN app_ads_map aam ON aam.app_ads_entry = aae.id
LEFT JOIN pub_domains pd ON pd.id = aam.pub_domain
WHERE pd.crawled_at::date = aam.updated_at::date AND pd.crawl_result = 1
WITH DATA;

-- View indexes:
CREATE UNIQUE INDEX app_ads_view_developer_domain_url_idx 
ON public.app_ads_view USING btree (developer_domain_url, ad_domain, publisher_id, ad_domain_url, relationship
);


-- public.audit_dates source

CREATE MATERIALIZED VIEW public.audit_dates
TABLESPACE pg_default
AS WITH sa AS (
    SELECT
        store_apps_audit.stamp::date AS updated_date,
        'store_apps'::text AS table_name,
        count(1) AS updated_count
    FROM logging.store_apps_audit
    GROUP BY (store_apps_audit.stamp::date)
)
SELECT
    sa.updated_date,
    sa.table_name,
    sa.updated_count
FROM sa
WITH DATA;

-- View indexes:
CREATE UNIQUE INDEX audit_dates_updated_date_idx ON public.audit_dates USING btree (updated_date, table_name);

--for internal dashboard checking last_updated_at and created_ats
--DROP MATERIALIZED VIEW store_apps_updated_at;
CREATE MATERIALIZED VIEW store_apps_updated_at AS
WITH my_dates AS
  (
    SELECT
        store,
          generate_series(
            CURRENT_DATE - INTERVAL '365 days',
            CURRENT_DATE,
            '1 day'::INTERVAL
        )::date AS date
    FROM
        generate_series(
            1,
            2,
            1
        ) AS num_series(store)
),
updated_dates AS
  (
    SELECT
        store,
          updated_at::date AS last_updated_date,
          count(1) AS last_updated_count
    FROM
        store_apps
    WHERE
        updated_at >= CURRENT_DATE - INTERVAL '365 days'
    GROUP BY
        store,
            updated_at::date
)
SELECT
    my_dates.store AS store,
       my_dates.date AS date,
       updated_dates.last_updated_count,
       audit_dates.updated_count
FROM
    my_dates
LEFT JOIN updated_dates ON
    my_dates.date = updated_dates.last_updated_date
    AND my_dates.store = updated_dates.store
LEFT JOIN audit_dates ON
    my_dates.date = audit_dates.updated_date
ORDER BY
    my_dates.date DESC ;


CREATE UNIQUE INDEX idx_store_apps_updated_at
ON store_apps_updated_at (store, date);


CREATE MATERIALIZED VIEW store_apps_created_at AS
WITH my_dates AS
  (
    SELECT
        store,
          generate_series(
            CURRENT_DATE - INTERVAL '365 days',
            CURRENT_DATE,
            '1 day'::INTERVAL
        )::date AS date
    FROM
        generate_series(
            1,
            2,
            1
        ) AS num_series(store)
),
created_dates AS
  (
    SELECT
          sa.store,
          sa.created_at::date AS created_date,
          sas.crawl_source,
          count(1) AS created_count
    FROM
        store_apps sa
    LEFT JOIN logging.store_app_sources sas
    ON
        sas.store_app = sa.id
        AND sas.store = sa.store
    WHERE
        created_at >= CURRENT_DATE - INTERVAL '365 days'
    GROUP BY
        sa.store,
        sa.created_at::date,
        sas.crawl_source
)
SELECT       
    my_dates.store AS store,
    my_dates.date AS date,
    created_dates.crawl_source,
    created_dates.created_count
FROM
    my_dates
LEFT JOIN created_dates ON
    my_dates.date = created_dates.created_date
    AND my_dates.store = created_dates.store
;

CREATE UNIQUE INDEX idx_store_apps_created_at
ON store_apps_created_at (store, date, crawl_source);



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


CREATE MATERIALIZED VIEW category_mapping AS
SELECT
    DISTINCT 
    original_category,
       CASE
            WHEN mapped_category IN ('action', 'casual', 'adventure', 'arcade', 'board', 'card', 'casino', 'puzzle', 'racing', 'simulation' , 'strategy', 'trivia', 'word') THEN 'game_' || mapped_category
        WHEN mapped_category = 'news_and_magazines' THEN 'news'
        WHEN mapped_category = 'educational' THEN 'education'
        WHEN mapped_category = 'book' THEN 'books_and_reference'
        WHEN mapped_category = 'navigation' THEN 'maps_and_navigation'
        WHEN mapped_category = 'music' THEN 'music_and_audio'
        WHEN mapped_category = 'photography' THEN 'photo_and_video'
        WHEN mapped_category = 'reference' THEN 'books_and_reference'
        WHEN mapped_category = 'role playing' THEN 'game_role_playing'
        WHEN mapped_category = 'social' THEN 'social networking'
        WHEN mapped_category = 'travel' THEN 'travel_and_local'
        WHEN mapped_category = 'utilities' THEN 'tools'
        WHEN mapped_category = 'video players_and_editors' THEN 'video_players'
        WHEN mapped_category = 'graphics_and_design' THEN 'art_and_design'
        WHEN mapped_category = 'parenting' THEN 'family'
        WHEN mapped_category IS NULL THEN 'N/A'
        ELSE mapped_category
    END AS mapped_category
FROM
    (
    SELECT
        DISTINCT 
        category AS original_category,
        regexp_replace(lower(category),
        ' & ',
        '_and_') AS mapped_category
    FROM
        store_apps
) AS sub;


--Make index of just categories
CREATE MATERIALIZED VIEW mv_app_categories AS
SELECT
    sa.store,
    cm.mapped_category AS category,
    COUNT(*) AS app_count
FROM
    store_apps sa
JOIN category_mapping cm ON
    sa.category = cm.original_category
GROUP BY
    sa.store,
    cm.mapped_category
ORDER BY
    sa.store,
    cm.mapped_category
;

--DROP MATERIALIZED VIEW apps_new_weekly;
CREATE MATERIALIZED VIEW apps_new_weekly AS
WITH RankedApps AS (
    SELECT
        *,
        ROW_NUMBER() OVER(
            PARTITION BY store,
            mapped_category
        ORDER BY 
            installs DESC NULLS LAST, 
            rating_count DESC NULLS LAST
        ) AS rn
    FROM
        store_apps sa
    JOIN category_mapping cm 
    ON
        sa.category = cm.original_category
    WHERE
        sa.release_date >= current_date - INTERVAL '7 days'
        AND crawl_result = 1
)
SELECT
    *
FROM
    RankedApps
WHERE
    rn <= 100
WITH DATA
;
--REFRESH MATERIALIZED VIEW apps_new_weekly ;

--DROP INDEX IF EXISTS idx_apps_new_weekly;
CREATE UNIQUE INDEX idx_apps_new_weekly
ON apps_new_weekly (store, mapped_category, store_id);


--DROP MATERIALIZED VIEW IF EXISTS apps_new_monthly;
CREATE MATERIALIZED VIEW apps_new_monthly AS
WITH RankedApps AS (
    SELECT
        *,
        ROW_NUMBER() OVER(
            PARTITION BY store,
            mapped_category
        ORDER BY 
            installs DESC NULLS LAST, 
            rating_count DESC NULLS LAST
        ) AS rn
    FROM
        store_apps sa
    JOIN category_mapping cm 
    ON
        sa.category = cm.original_category
    WHERE
        sa.release_date >= current_date - INTERVAL '30 days'
        AND crawl_result = 1
)
SELECT
    *
FROM
    RankedApps
WHERE
    rn <= 100
WITH DATA
;
DROP INDEX IF EXISTS idx_apps_new_monthly;
CREATE UNIQUE INDEX idx_apps_new_monthly
ON apps_new_monthly (store, mapped_category, store_id);


DROP MATERIALIZED VIEW IF EXISTS apps_new_yearly;
CREATE MATERIALIZED VIEW apps_new_yearly AS
WITH RankedApps AS (
    SELECT
        *,
        ROW_NUMBER() OVER(
            PARTITION BY store,
            mapped_category
        ORDER BY 
            installs DESC NULLS LAST, 
            rating_count DESC NULLS LAST
        ) AS rn
    FROM
        store_apps sa
    JOIN category_mapping cm 
    ON
        sa.category = cm.original_category
    WHERE
        sa.release_date >= current_date - INTERVAL '365 days'
        AND crawl_result = 1
)
SELECT
    *
FROM
    RankedApps
WHERE
    rn <= 100
WITH DATA
;
DROP INDEX IF EXISTS idx_apps_new_yearly;
CREATE UNIQUE INDEX idx_apps_new_yearly
ON apps_new_yearly (store, mapped_category, store_id);


        
--DROP MATERIALIZED VIEW top_categories;
CREATE MATERIALIZED VIEW top_categories AS
WITH rankedapps AS (
    SELECT
        *, 
        ROW_NUMBER() OVER(
            PARTITION BY store,
            mapped_category
        ORDER BY 
            installs DESC NULLS LAST, 
            rating_count DESC NULLS LAST
        ) AS rn
    FROM    
        store_apps sa
    JOIN category_mapping cm
        ON
            sa.category = cm.original_category
    WHERE
        sa.crawl_result = 1
)

SELECT * FROM
    rankedapps
WHERE
    rn <= 50
WITH DATA;

--REFRESH MATERIALIZED VIEW top_categories ;

DROP INDEX IF EXISTS idx_top_categories;
CREATE UNIQUE INDEX idx_top_categories
ON top_categories (store, mapped_category, store_id);
