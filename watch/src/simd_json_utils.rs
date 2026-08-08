use serde::de::DeserializeOwned;
use std::cell::RefCell;

thread_local! {
    static SIMD_JSON_BUFFER: RefCell<Vec<u8>> = RefCell::new(Vec::with_capacity(65536));
}

/// Parse JSON using simd-json for SIMD acceleration when available.
/// Uses a thread-local buffer to avoid allocation per message.
#[inline]
pub fn parse_json_simd<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, simd_json::Error> {
    SIMD_JSON_BUFFER.with(|buf| {
        let mut buf = buf.borrow_mut();
        buf.clear();
        buf.extend_from_slice(bytes);
        simd_json::from_slice(&mut buf)
    })
}
