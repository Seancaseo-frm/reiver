# Reiver Integrations

This directory contains integrations organized by where they run (server vs agent).

## Structure

```
integrations/
├── server/           # Server-side integrations (webhooks, API polling, etc.)
│   ├── Cargo.toml   # Workspace for server integrations
│   └── webhooks/    # Webhook handlers crate
│       ├── Cargo.toml
│       └── src/
│           ├── lib.rs
│           ├── common.rs         # Shared types and traits
│           └── feature_flags/    # Feature flag webhook handlers
│               ├── mod.rs
│               └── launchdarkly.rs  # Example migrated handler
│
└── agent/            # Agent-side integrations (database collectors, etc.)
    └── (future: database collectors, service monitors, etc.)
```

## Migration Status

### ✅ Completed
- Created integration structure with server/agent separation
- Created `reiver-integrations-server-webhooks` crate
- Created `FeatureFlagEventStorage` trait interface
- Migrated LaunchDarkly webhook handler as example

### ⏳ In Progress
- Migrating remaining feature flag webhook handlers (9 remaining)
- Implementing `FeatureFlagEventStorage` trait in main codebase
- Updating router to use new handlers

### 📋 Next Steps

1. **Implement trait in main codebase** (`src/api/events.rs`):
   ```rust
   impl FeatureFlagEventStorage for AppState {
       async fn store_flag_change(&self, event: FeatureFlagChangeEvent) -> Result<Uuid, String> {
           // Move handle_feature_flag_change_internal logic here
       }
   }
   ```

2. **Add dependency to main Cargo.toml**:
   ```toml
   [dependencies]
   reiver-integrations-server-webhooks = { path = "integrations/server/webhooks" }
   ```

3. **Migrate remaining handlers** (unleash, flagsmith, configcat, split, cloudbees, optimizely, gofeatureflag, flipt, growthbook)

4. **Update router** to use handlers from integration crate

## Architecture

Integrations use a trait-based interface to decouple handler logic from server implementation:

- **Handlers** (in integration crate): Parse webhook payloads, convert to internal format
- **Trait** (`FeatureFlagEventStorage`): Defines interface for storing events
- **Implementation** (in main codebase): Provides database/storage access

This allows:
- Integration handlers to be independent and testable
- Server implementation to evolve without breaking handlers
- Clear separation of concerns

