CREATE MATERIALIZED VIEW public.networks_with_app_metrics
TABLESPACE pg_default
AS WITH pnv AS (
    SELECT DISTINCT
        publisher_network_view.publisher_domain_url,
        publisher_network_view.relationship
    FROM publisher_network_view
),

tot AS (
    SELECT
        pnv.relationship,
        pdwi.store,
        pdwi.category,
        count(DISTINCT pnv.publisher_domain_url) AS total_publisher_urls,
        sum(pdwi.app_count) AS total_apps,
        sum(pdwi.total_installs) AS total_installs,
        sum(pdwi.total_review) AS total_reviews
    FROM publisher_domain_with_installs AS pdwi
    INNER JOIN pnv
        ON pdwi.url::text = pnv.publisher_domain_url::text
    WHERE (pdwi.url::text IN (
        SELECT publisher_network_view.publisher_domain_url
        FROM publisher_network_view
    ))
    GROUP BY pnv.relationship, pdwi.store, pdwi.category
),

nwm AS (
    SELECT
        pnv.ad_domain_url,
        pnv.relationship,
        pdwi.store,
        pdwi.category,
        count(DISTINCT pnv.publisher_domain_url) AS publisher_urls,
        sum(pdwi.app_count) AS apps,
        sum(pdwi.total_review) AS reviews,
        sum(pdwi.total_installs) AS installs
    FROM publisher_network_view AS pnv
    INNER JOIN
        publisher_domain_with_installs AS pdwi
        ON pnv.publisher_domain_url::text = pdwi.url::text
    GROUP BY
        cube (pnv.ad_domain_url, pnv.relationship, pdwi.store, pdwi.category)
)

SELECT
    nwm.ad_domain_url,
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
LEFT JOIN
    tot
    ON
        nwm.store = tot.store
        AND nwm.category = tot.category
        AND nwm.relationship::text = tot.relationship::text
WHERE
    nwm.relationship IS NOT NULL
    AND nwm.ad_domain_url IS NOT NULL
    AND nwm.category IS NOT NULL
    AND nwm.store IS NOT NULL
ORDER BY
    tot.total_publisher_urls DESC NULLS LAST, nwm.publisher_urls DESC NULLS LAST
WITH DATA;

-- View indexes:
CREATE UNIQUE INDEX networks_with_app_metrics_idx
ON public.networks_with_app_metrics USING btree (
    ad_domain_url, relationship, store, category
);


-- public.publisher_domain_with_installs source

CREATE MATERIALIZED VIEW public.publisher_domain_with_installs
TABLESPACE pg_default
AS SELECT
    pd.id AS pub_domain_id,
    pd.url,
    sa.store,
    CASE
        WHEN
            regexp_replace(lower(sa.category::text), ' & '::text, '_and_'::text)
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
            THEN
                'game_'::text
                || regexp_replace(
                    lower(sa.category::text), ' & '::text, '_and_'::text
                )
        WHEN
            regexp_replace(lower(sa.category::text), ' & '::text, '_and_'::text)
            = 'news'::text
            THEN 'magazines_and_newspapers'::text
        WHEN
            regexp_replace(lower(sa.category::text), ' & '::text, '_and_'::text)
            = 'educational'::text
            THEN 'education'::text
        WHEN
            regexp_replace(lower(sa.category::text), ' & '::text, '_and_'::text)
            = 'book'::text
            THEN 'books_and_reference'::text
        WHEN
            regexp_replace(lower(sa.category::text), ' & '::text, '_and_'::text)
            = 'navigation'::text
            THEN 'maps_and_navigation'::text
        WHEN
            regexp_replace(lower(sa.category::text), ' & '::text, '_and_'::text)
            = 'music'::text
            THEN 'music_and_audio'::text
        WHEN
            regexp_replace(lower(sa.category::text), ' & '::text, '_and_'::text)
            = 'photography'::text
            THEN 'photo_and_video'::text
        WHEN
            regexp_replace(lower(sa.category::text), ' & '::text, '_and_'::text)
            = 'reference'::text
            THEN 'books_and_reference'::text
        WHEN
            regexp_replace(lower(sa.category::text), ' & '::text, '_and_'::text)
            = 'role playing'::text
            THEN 'game_role_playing'::text
        WHEN
            regexp_replace(lower(sa.category::text), ' & '::text, '_and_'::text)
            = 'social'::text
            THEN 'social networking'::text
        WHEN
            regexp_replace(lower(sa.category::text), ' & '::text, '_and_'::text)
            = 'travel'::text
            THEN 'travel_and_local'::text
        WHEN
            regexp_replace(lower(sa.category::text), ' & '::text, '_and_'::text)
            = 'utilities'::text
            THEN 'tools'::text
        WHEN
            regexp_replace(lower(sa.category::text), ' & '::text, '_and_'::text)
            = 'video players_and_editors'::text
            THEN 'video_players'::text
        ELSE
            regexp_replace(lower(sa.category::text), ' & '::text, '_and_'::text)
    END AS category,
    count(*) AS app_count,
    sum(
        coalesce(
            nullif(sa.installs, 'NaN'::double precision), 0::double precision
        )
    ) AS total_installs,
    sum(
        coalesce(
            nullif(sa.review_count, 'NaN'::double precision),
            0::double precision
        )
    ) AS total_review
FROM pub_domains AS pd
LEFT JOIN app_urls_map AS aum ON pd.id = aum.pub_domain
LEFT JOIN store_apps AS sa ON aum.store_app = sa.id
WHERE sa.crawl_result = 1
GROUP BY
    pd.id, pd.url, sa.store, (
        CASE
            WHEN
                regexp_replace(
                    lower(sa.category::text), ' & '::text, '_and_'::text
                )
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
                THEN
                    'game_'::text
                    || regexp_replace(
                        lower(sa.category::text), ' & '::text, '_and_'::text
                    )
            WHEN
                regexp_replace(
                    lower(sa.category::text), ' & '::text, '_and_'::text
                )
                = 'news'::text
                THEN 'magazines_and_newspapers'::text
            WHEN
                regexp_replace(
                    lower(sa.category::text), ' & '::text, '_and_'::text
                )
                = 'educational'::text
                THEN 'education'::text
            WHEN
                regexp_replace(
                    lower(sa.category::text), ' & '::text, '_and_'::text
                )
                = 'book'::text
                THEN 'books_and_reference'::text
            WHEN
                regexp_replace(
                    lower(sa.category::text), ' & '::text, '_and_'::text
                )
                = 'navigation'::text
                THEN 'maps_and_navigation'::text
            WHEN
                regexp_replace(
                    lower(sa.category::text), ' & '::text, '_and_'::text
                )
                = 'music'::text
                THEN 'music_and_audio'::text
            WHEN
                regexp_replace(
                    lower(sa.category::text), ' & '::text, '_and_'::text
                )
                = 'photography'::text
                THEN 'photo_and_video'::text
            WHEN
                regexp_replace(
                    lower(sa.category::text), ' & '::text, '_and_'::text
                )
                = 'reference'::text
                THEN 'books_and_reference'::text
            WHEN
                regexp_replace(
                    lower(sa.category::text), ' & '::text, '_and_'::text
                )
                = 'role playing'::text
                THEN 'game_role_playing'::text
            WHEN
                regexp_replace(
                    lower(sa.category::text), ' & '::text, '_and_'::text
                )
                = 'social'::text
                THEN 'social networking'::text
            WHEN
                regexp_replace(
                    lower(sa.category::text), ' & '::text, '_and_'::text
                )
                = 'travel'::text
                THEN 'travel_and_local'::text
            WHEN
                regexp_replace(
                    lower(sa.category::text), ' & '::text, '_and_'::text
                )
                = 'utilities'::text
                THEN 'tools'::text
            WHEN
                regexp_replace(
                    lower(sa.category::text), ' & '::text, '_and_'::text
                )
                = 'video players_and_editors'::text
                THEN 'video_players'::text
            ELSE
                regexp_replace(
                    lower(sa.category::text), ' & '::text, '_and_'::text
                )
        END
    )
ORDER BY
    (
        sum(
            coalesce(
                nullif(sa.installs, 'NaN'::double precision),
                0::double precision
            )
        )
    ) DESC
WITH DATA;

-- View indexes:
CREATE UNIQUE INDEX publisher_domain_with_installs_unique_indx
ON public.publisher_domain_with_installs USING btree (
    pub_domain_id, url, store, category
);


-- public.publisher_network_view source

CREATE MATERIALIZED VIEW public.publisher_network_view
TABLESPACE pg_default
AS SELECT DISTINCT
    av.developer_domain_url AS publisher_domain_url,
    av.ad_domain_url,
    av.relationship
FROM app_ads_view AS av
WHERE av.developer_domain_crawled_at::date = av.txt_entry_crawled_at::date
WITH DATA;

-- View indexes:
CREATE UNIQUE INDEX publisher_network_view_uniq_idx
ON public.publisher_network_view USING btree (
    publisher_domain_url, ad_domain_url, relationship
);


-- public.publisher_url_developer_ids_uniques source

CREATE MATERIALIZED VIEW public.publisher_url_developer_ids_uniques
TABLESPACE pg_default
AS WITH uniq_pub_urls AS (
    SELECT
        app_ads_view.ad_domain_url,
        app_ads_view.publisher_id,
        count(
            DISTINCT app_ads_view.developer_domain_url
        ) AS unique_publisher_urls
    FROM app_ads_view
    WHERE app_ads_view.relationship::text = 'DIRECT'::text
    GROUP BY app_ads_view.ad_domain_url, app_ads_view.publisher_id
),

dev_url_map AS (
    SELECT DISTINCT
        d.developer_id,
        pd.url AS pub_domain_url
    FROM store_apps AS sa
    LEFT JOIN developers AS d ON sa.developer = d.id
    LEFT JOIN app_urls_map AS aum ON sa.id = aum.store_app
    LEFT JOIN pub_domains AS pd ON aum.pub_domain = pd.id
    WHERE sa.crawl_result = 1 AND pd.crawl_result = 1
),

uniq_dev_ids AS (
    SELECT
        aav.ad_domain_url,
        aav.publisher_id,
        count(DISTINCT dum.developer_id) AS unique_developer_ids
    FROM app_ads_view AS aav
    LEFT JOIN
        dev_url_map AS dum
        ON dum.pub_domain_url::text = aav.developer_domain_url::text
    WHERE aav.relationship::text = 'DIRECT'::text
    GROUP BY aav.ad_domain_url, aav.publisher_id
)

SELECT
    upu.ad_domain_url,
    upu.publisher_id,
    upu.unique_publisher_urls,
    udi.unique_developer_ids,
    CASE
        WHEN
            upu.unique_publisher_urls <= 1 OR udi.unique_developer_ids <= 1
            THEN 1
        ELSE 0
    END AS is_unique
FROM uniq_pub_urls AS upu
FULL JOIN
    uniq_dev_ids AS udi
    ON
        upu.ad_domain_url::text = udi.ad_domain_url::text
        AND upu.publisher_id::text = udi.publisher_id::text
WITH DATA;


CREATE MATERIALIZED VIEW category_mapping AS
SELECT DISTINCT
    original_category,
    CASE
        WHEN
            mapped_category IN (
                'action',
                'casual',
                'adventure',
                'arcade',
                'board',
                'card',
                'casino',
                'puzzle',
                'racing',
                'simulation',
                'strategy',
                'trivia',
                'word'
            )
            THEN 'game_' || mapped_category
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
        SELECT DISTINCT
            category AS original_category,
            regexp_replace(
                lower(category),
                ' & ',
                '_and_'
            ) AS mapped_category
        FROM
            store_apps
    ) AS sub;
-- Create indexes:
CREATE UNIQUE INDEX category_mapping_idx
ON public.category_mapping USING btree (
    original_category, mapped_category
);
