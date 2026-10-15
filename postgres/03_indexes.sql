CREATE INDEX IF NOT EXISTS
    netflix_titles_silver_embedding_idx
ON public.netflix_titles_silver
USING hnsw (embedding vector_cosine_ops);