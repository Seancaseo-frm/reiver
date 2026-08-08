use bb8::Pool;
use bb8_redis::RedisConnectionManager;
use std::sync::Arc;

use crate::config::Config;
use crate::db::DbPool;

pub type RedisPool = Pool<RedisConnectionManager>;

// =============================================================================
// AuthContext -- trait for auth utility functions
// =============================================================================
// Each product's state struct implements this so that core::auth helpers
// (extract_auth_user, authenticate_request, etc.) work generically.

pub trait AuthContext {
    fn db(&self) -> &Arc<DbPool>;
    fn redis(&self) -> &Arc<RedisPool>;
    fn config(&self) -> &Arc<Config>;
}

// Blanket impl so `&Arc<S>` also satisfies AuthContext when S does.
// This lets handlers pass `&state` where state: Arc<ProductState>.
impl<T: AuthContext> AuthContext for Arc<T> {
    fn db(&self) -> &Arc<DbPool> {
        (**self).db()
    }
    fn redis(&self) -> &Arc<RedisPool> {
        (**self).redis()
    }
    fn config(&self) -> &Arc<Config> {
        (**self).config()
    }
}
