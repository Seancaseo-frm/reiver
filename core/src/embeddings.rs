use std::sync::Arc;

use fastembed::{EmbeddingModel, TextEmbedding, TextInitOptions};

/// Local embedding model for knowledge base vector search.
///
/// Uses `BAAI/bge-small-en-v1.5` (384 dimensions) via ONNX Runtime.
/// The model (~130 MB) is downloaded on first use and cached locally.
/// All inference runs in-process — no external API calls.
pub struct KbEmbedder {
    model: Option<Arc<std::sync::Mutex<TextEmbedding>>>,
}

impl KbEmbedder {
    pub fn new() -> anyhow::Result<Self> {
        let model = TextEmbedding::try_new(
            TextInitOptions::new(EmbeddingModel::BGESmallENV15)
                .with_show_download_progress(true),
        )?;
        Ok(Self {
            model: Some(Arc::new(std::sync::Mutex::new(model))),
        })
    }

    /// Create a no-op embedder that returns zero vectors.
    /// Useful in tests that don't exercise knowledge base search.
    pub fn noop() -> Self {
        Self { model: None }
    }

    /// Generate embeddings for a batch of texts.
    ///
    /// Runs on a blocking thread to avoid stalling the Tokio runtime
    /// (ONNX inference is CPU-bound).
    pub async fn embed(&self, texts: Vec<String>) -> anyhow::Result<Vec<Vec<f32>>> {
        let model = match &self.model {
            Some(m) => m.clone(),
            None => return Ok(texts.iter().map(|_| vec![0.0; Self::embedding_dim()]).collect()),
        };
        tokio::task::spawn_blocking(move || {
            let mut model = model
                .lock()
                .map_err(|e| anyhow::anyhow!("embedding model lock poisoned: {e}"))?;
            let embeddings = model.embed(texts, None)?;
            Ok(embeddings)
        })
        .await?
    }

    pub fn embedding_dim() -> usize {
        384
    }
}
