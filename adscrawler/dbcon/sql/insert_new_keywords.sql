WITH distinct_sq AS (
    SELECT DISTINCT search_term
    FROM agadmin.search_queries
),
normalized_sq AS (
    SELECT
        search_term AS original_term,
        TRIM(
            REGEXP_REPLACE(
                REPLACE(
                    REPLACE(
                        LOWER(search_term),
                        '"', ' '
                    ),
                    '+', ' '
                ),
                '\s+',
                ' ',
                'g'
            )
        ) AS normalized_term
    FROM distinct_sq
),
new_keywords AS (
    SELECT sq.normalized_term
    FROM normalized_sq AS sq
    LEFT JOIN keywords AS k
        ON sq.normalized_term = k.keyword_text
    WHERE
        k.id IS NULL
        AND sq.normalized_term !~ '^\d+$'
        AND sq.normalized_term !~ '^[\d\s]+$'  -- also exclude pure number groups
        AND sq.normalized_term <> ''            -- exclude empty strings after normalization
        AND LENGTH(sq.normalized_term) <= 255   -- respect varchar(255) constraint
)
INSERT INTO public.keywords (keyword_text)
SELECT normalized_term
FROM new_keywords
ON CONFLICT (keyword_text) DO NOTHING;
