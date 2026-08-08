use datafusion::error::DataFusionError;

pub type EvalResult<T> = Result<T, EvalError>;

#[derive(Debug, thiserror::Error)]
pub enum EvalError {
    #[error("PromQL parse error: {0}")]
    Parse(String),

    #[error("unsupported PromQL expression: {0}")]
    Unsupported(String),

    #[error("invalid range: {0}")]
    InvalidRange(String),

    #[error("DataFusion error: {0}")]
    DataFusion(#[from] DataFusionError),

    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("invalid query: {0}")]
    Invalid(String),

    #[error("ClickHouse fetch error: {0}")]
    Fetch(String),

    #[error("internal error: {0}")]
    Internal(String),
}

impl From<EvalError> for DataFusionError {
    fn from(e: EvalError) -> Self {
        DataFusionError::External(Box::new(e))
    }
}
