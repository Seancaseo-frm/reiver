use futures::StreamExt;
use tokio::io::AsyncBufReadExt;
use tokio_util::io::StreamReader;

/// Stream a ClickHouse JSONEachRow response into parsed JSON values.
///
/// Uses `Content-Length` to pre-allocate the result vector and parses
/// directly from a reusable byte buffer via `serde_json::from_slice`,
/// avoiding the full-body `String` allocation that `.text().await` performs.
///
/// Callers should check `response.status().is_success()` before calling
/// this function — on error responses, use `.text().await` to read the
/// small error body instead.
pub async fn stream_json_lines(
    response: reqwest::Response,
) -> Result<Vec<serde_json::Value>, std::io::Error> {
    let size_hint = response.content_length().unwrap_or(0) as usize;
    let estimated_rows = (size_hint / 256).max(64);

    let reader = StreamReader::new(
        response
            .bytes_stream()
            .map(|r| r.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))),
    );
    let mut reader = tokio::io::BufReader::new(reader);
    let mut buf = Vec::with_capacity(4096);
    let mut rows = Vec::with_capacity(estimated_rows);

    loop {
        buf.clear();
        let n = reader.read_until(b'\n', &mut buf).await?;
        if n == 0 {
            break;
        }
        let trimmed = buf.trim_ascii();
        if trimmed.is_empty() {
            continue;
        }
        let value: serde_json::Value = serde_json::from_slice(trimmed)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        rows.push(value);
    }
    Ok(rows)
}
