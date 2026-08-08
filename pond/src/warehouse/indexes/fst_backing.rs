//! Backing storage for FST data structures.
//!
//! Supports both owned bytes (used at build time when constructing FSTs)
//! and zero-copy mmap references (used at query time to avoid heap copies).

use memmap2::Mmap;
use std::sync::Arc;

/// Backing storage for FST data. Supports both owned bytes (build time)
/// and zero-copy mmap references (query time).
#[derive(Clone)]
pub enum FstBacking {
    Owned(Vec<u8>),
    Mmap {
        mmap: Arc<Mmap>,
        offset: usize,
        len: usize,
    },
}

impl std::fmt::Debug for FstBacking {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FstBacking::Owned(v) => f.debug_tuple("Owned").field(&v.len()).finish(),
            FstBacking::Mmap { offset, len, .. } => {
                f.debug_struct("Mmap")
                    .field("offset", offset)
                    .field("len", len)
                    .finish()
            }
        }
    }
}

impl FstBacking {
    pub fn owned(bytes: Vec<u8>) -> Self {
        Self::Owned(bytes)
    }

    pub fn from_mmap(mmap: Arc<Mmap>, offset: usize, len: usize) -> Self {
        Self::Mmap { mmap, offset, len }
    }
}

impl AsRef<[u8]> for FstBacking {
    fn as_ref(&self) -> &[u8] {
        match self {
            FstBacking::Owned(v) => v.as_ref(),
            FstBacking::Mmap { mmap, offset, len } => &mmap[*offset..*offset + *len],
        }
    }
}
