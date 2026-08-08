-- pgvector must be installed by a superuser before this migration runs:
--   CREATE EXTENSION IF NOT EXISTS vector;
--
-- Verify it exists; abort with a clear message if missing.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'vector') THEN
        RAISE EXCEPTION 'pgvector extension is not installed. Run "CREATE EXTENSION vector;" as a database superuser first.';
    END IF;
END
$$;

CREATE TABLE knowledge_base_documents (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    title TEXT NOT NULL,
    category TEXT NOT NULL,
    source_type TEXT NOT NULL DEFAULT 'manual',
    original_content TEXT,
    original_filename TEXT,
    severity TEXT NOT NULL DEFAULT 'info',
    enabled BOOLEAN NOT NULL DEFAULT true,
    embedding_status TEXT NOT NULL DEFAULT 'pending',
    embedding_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE knowledge_base_chunks (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    document_id UUID NOT NULL REFERENCES knowledge_base_documents(id) ON DELETE CASCADE,
    content TEXT NOT NULL,
    chunk_index INTEGER NOT NULL,
    embedding vector(384) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_kb_chunks_embedding ON knowledge_base_chunks
    USING hnsw (embedding vector_cosine_ops);
CREATE INDEX idx_kb_chunks_document ON knowledge_base_chunks (document_id);
