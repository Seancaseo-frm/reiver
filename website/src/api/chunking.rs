/// Maximum characters per chunk (~500 tokens).
const MAX_CHUNK_CHARS: usize = 2000;
/// Overlap between consecutive chunks to preserve context at boundaries.
const OVERLAP_CHARS: usize = 200;

/// A text chunk with its positional index within the source document.
pub struct TextChunk {
    pub text: String,
    pub index: usize,
}

/// Split `text` into overlapping chunks, prepending `title` to each for context.
///
/// Splitting strategy:
/// 1. Paragraph boundaries (double newline)
/// 2. Sentence boundaries (period + space)
/// 3. Hard cut at `MAX_CHUNK_CHARS`
pub fn chunk_text(title: &str, text: &str) -> Vec<TextChunk> {
    let text = text.trim();
    if text.is_empty() {
        return Vec::new();
    }

    // Cap prefix to half the chunk budget so there's always room for content.
    let full_prefix = format!("[{title}] ");
    let prefix = if full_prefix.len() > MAX_CHUNK_CHARS / 2 {
        let mut cut = MAX_CHUNK_CHARS / 2;
        while cut > 0 && !full_prefix.is_char_boundary(cut) {
            cut -= 1;
        }
        &full_prefix[..cut]
    } else {
        &full_prefix
    };

    if prefix.len() + text.len() <= MAX_CHUNK_CHARS {
        return vec![TextChunk {
            text: format!("{prefix}{text}"),
            index: 0,
        }];
    }

    let mut chunks = Vec::new();
    let mut start = 0;
    let bytes = text.as_bytes();

    while start < text.len() {
        let budget = MAX_CHUNK_CHARS.saturating_sub(prefix.len()).max(1);
        let mut end = (start + budget).min(text.len());

        // Snap to a char boundary.
        while end < text.len() && !text.is_char_boundary(end) {
            end += 1;
        }

        if end < text.len() {
            // Try to break at a paragraph boundary.
            if let Some(pos) = rfind_pattern(&bytes[start..end], b"\n\n") {
                let candidate = start + pos + 2;
                if candidate > start + budget / 4 {
                    end = candidate;
                }
            } else if let Some(pos) = rfind_pattern(&bytes[start..end], b". ") {
                let candidate = start + pos + 2;
                if candidate > start + budget / 4 {
                    end = candidate;
                }
            }
        }

        // Snap end to char boundary again after adjustment.
        while end > start && !text.is_char_boundary(end) {
            end -= 1;
        }
        if end == start {
            end = (start + budget).min(text.len());
            while end < text.len() && !text.is_char_boundary(end) {
                end += 1;
            }
        }

        let chunk_text = format!("{prefix}{}", &text[start..end].trim());
        chunks.push(TextChunk {
            text: chunk_text,
            index: chunks.len(),
        });

        if end >= text.len() {
            break;
        }

        let next_start = if end > OVERLAP_CHARS {
            end - OVERLAP_CHARS
        } else {
            end
        };

        // Snap to char boundary.
        let mut next = next_start;
        while next < text.len() && !text.is_char_boundary(next) {
            next += 1;
        }
        start = next;
    }

    chunks
}

fn rfind_pattern(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.len() > haystack.len() {
        return None;
    }
    for i in (0..=(haystack.len() - needle.len())).rev() {
        if &haystack[i..i + needle.len()] == needle {
            return Some(i);
        }
    }
    None
}

/// Extract text from a PDF file in memory.
pub fn extract_text_from_pdf(data: &[u8]) -> anyhow::Result<String> {
    pdf_extract::extract_text_from_mem(data)
        .map_err(|e| anyhow::anyhow!("PDF text extraction failed: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn short_text_single_chunk() {
        let chunks = chunk_text("Test", "Hello world.");
        assert_eq!(chunks.len(), 1);
        assert!(chunks[0].text.starts_with("[Test] "));
        assert_eq!(chunks[0].index, 0);
    }

    #[test]
    fn long_text_multiple_chunks() {
        let text = "A ".repeat(2000);
        let chunks = chunk_text("Doc", &text);
        assert!(chunks.len() > 1);
        for (i, c) in chunks.iter().enumerate() {
            assert_eq!(c.index, i);
            assert!(c.text.starts_with("[Doc] "));
        }
    }

    #[test]
    fn very_long_title_does_not_infinite_loop() {
        let title = "X".repeat(3000);
        let chunks = chunk_text(&title, "Some content here.");
        assert!(!chunks.is_empty());
        assert!(chunks[0].text.len() <= MAX_CHUNK_CHARS + 10);
    }

    #[test]
    fn empty_and_whitespace() {
        assert!(chunk_text("T", "").is_empty());
        assert!(chunk_text("T", "   ").is_empty());
    }
}
