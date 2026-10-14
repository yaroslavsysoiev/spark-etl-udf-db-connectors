CREATE TABLE IF NOT EXISTS public.netflix_titles_silver
(
    show_id TEXT PRIMARY KEY,
    title TEXT,
    country TEXT,
    country_code TEXT,
    description TEXT,
    embedding VECTOR(384)
);