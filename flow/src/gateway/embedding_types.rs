//! OpenAI-compatible request/response types for the embeddings endpoint.

use serde::{Deserialize, Serialize};

/// Input that deserializes from either a single string or an array of strings.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(untagged)]
pub enum EmbeddingInput {
    Single(String),
    Multiple(Vec<String>),
}

impl EmbeddingInput {
    /// Return the input texts as a slice-friendly `Vec<&str>`.
    pub fn texts(&self) -> Vec<&str> {
        match self {
            EmbeddingInput::Single(s) => vec![s.as_str()],
            EmbeddingInput::Multiple(v) => v.iter().map(|s| s.as_str()).collect(),
        }
    }

    /// Return a mutable view of the underlying strings for in-place updates
    /// (e.g. after PII masking produces owned replacements).
    pub fn texts_mut(&mut self) -> Vec<&mut String> {
        match self {
            EmbeddingInput::Single(s) => vec![s],
            EmbeddingInput::Multiple(v) => v.iter_mut().collect(),
        }
    }
}

/// OpenAI-compatible embedding request.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct EmbeddingRequest {
    pub model: String,

    pub input: EmbeddingInput,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub encoding_format: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub dimensions: Option<u32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub user: Option<String>,
}

/// OpenAI-compatible embedding response.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct EmbeddingResponse {
    pub object: String,
    pub data: Vec<EmbeddingData>,
    pub model: String,
    pub usage: EmbeddingUsage,
}

/// A single embedding vector in the response.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct EmbeddingData {
    pub object: String,
    pub embedding: Vec<f32>,
    pub index: usize,
}

/// Token usage for an embedding request.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct EmbeddingUsage {
    pub prompt_tokens: u32,
    pub total_tokens: u32,
}

impl EmbeddingRequest {
    /// Basic validation: model must be non-empty and input must contain at
    /// least one non-empty string.
    pub fn validate(&self) -> Result<(), Vec<String>> {
        let mut errors = Vec::new();

        if self.model.trim().is_empty() {
            errors.push("model field is required and cannot be empty".to_string());
        }

        let texts = self.input.texts();
        if texts.is_empty() {
            errors.push("input must contain at least one string".to_string());
        } else if texts.iter().all(|t| t.is_empty()) {
            errors.push("input must contain at least one non-empty string".to_string());
        }

        if let Some(ref fmt) = self.encoding_format {
            if fmt != "float" && fmt != "base64" {
                errors.push(format!(
                    "encoding_format must be 'float' or 'base64' (received '{}')",
                    fmt
                ));
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deserialize_single_string_input() {
        let json = r#"{"model": "text-embedding-3-small", "input": "hello world"}"#;
        let req: EmbeddingRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.input.texts(), vec!["hello world"]);
    }

    #[test]
    fn test_deserialize_array_input() {
        let json =
            r#"{"model": "text-embedding-3-small", "input": ["hello", "world"]}"#;
        let req: EmbeddingRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.input.texts(), vec!["hello", "world"]);
    }

    #[test]
    fn test_validate_empty_model() {
        let req = EmbeddingRequest {
            model: "".to_string(),
            input: EmbeddingInput::Single("hello".to_string()),
            encoding_format: None,
            dimensions: None,
            user: None,
        };
        let errors = req.validate().unwrap_err();
        assert!(errors.iter().any(|e| e.contains("model field is required")));
    }

    #[test]
    fn test_validate_empty_input() {
        let req = EmbeddingRequest {
            model: "text-embedding-3-small".to_string(),
            input: EmbeddingInput::Multiple(vec![]),
            encoding_format: None,
            dimensions: None,
            user: None,
        };
        let errors = req.validate().unwrap_err();
        assert!(errors
            .iter()
            .any(|e| e.contains("at least one string")));
    }

    #[test]
    fn test_validate_all_empty_strings() {
        let req = EmbeddingRequest {
            model: "text-embedding-3-small".to_string(),
            input: EmbeddingInput::Multiple(vec!["".to_string(), "".to_string()]),
            encoding_format: None,
            dimensions: None,
            user: None,
        };
        let errors = req.validate().unwrap_err();
        assert!(errors
            .iter()
            .any(|e| e.contains("non-empty string")));
    }

    #[test]
    fn test_validate_invalid_encoding_format() {
        let req = EmbeddingRequest {
            model: "text-embedding-3-small".to_string(),
            input: EmbeddingInput::Single("hello".to_string()),
            encoding_format: Some("invalid".to_string()),
            dimensions: None,
            user: None,
        };
        let errors = req.validate().unwrap_err();
        assert!(errors.iter().any(|e| e.contains("encoding_format")));
    }

    #[test]
    fn test_validate_valid_request() {
        let req = EmbeddingRequest {
            model: "text-embedding-3-small".to_string(),
            input: EmbeddingInput::Single("hello".to_string()),
            encoding_format: Some("float".to_string()),
            dimensions: Some(256),
            user: None,
        };
        assert!(req.validate().is_ok());
    }

    #[test]
    fn test_serialize_response() {
        let resp = EmbeddingResponse {
            object: "list".to_string(),
            data: vec![EmbeddingData {
                object: "embedding".to_string(),
                embedding: vec![0.1, 0.2, 0.3],
                index: 0,
            }],
            model: "text-embedding-3-small".to_string(),
            usage: EmbeddingUsage {
                prompt_tokens: 5,
                total_tokens: 5,
            },
        };
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("\"object\":\"list\""));
        assert!(json.contains("\"prompt_tokens\":5"));
    }

    #[test]
    fn test_deserialize_response() {
        let json = r#"{
            "object": "list",
            "data": [{"object": "embedding", "embedding": [0.1, 0.2], "index": 0}],
            "model": "text-embedding-3-small",
            "usage": {"prompt_tokens": 3, "total_tokens": 3}
        }"#;
        let resp: EmbeddingResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.data.len(), 1);
        assert_eq!(resp.usage.prompt_tokens, 3);
    }
}
