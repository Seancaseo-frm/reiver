use std::sync::Arc;

use ahash::AHashMap;
use anyhow::{Context, Result};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;
use wasmtime::Module;

use gno_rs::wasm::compiler::WasmCompiler;
use gno_rs::wasm::runtime::UdfRuntime;
use gno_rs::wasm::udf::Manifest;

use crate::db::DbPool;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionMode {
    SqlFunction,
    Job,
}

impl ExecutionMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::SqlFunction => "sql_function",
            Self::Job => "job",
        }
    }

    pub fn from_str(s: &str) -> Result<Self> {
        match s {
            "sql_function" => Ok(Self::SqlFunction),
            "job" => Ok(Self::Job),
            other => anyhow::bail!("unknown execution mode: {}", other),
        }
    }
}

pub struct CompiledUdf {
    pub id: Uuid,
    pub name: String,
    pub module: Module,
    pub manifest: Manifest,
    pub execution_mode: ExecutionMode,
    pub source_hash: String,
    pub fuel_limit: u64,
    pub timeout_secs: u32,
    pub schedule: Option<String>,
    pub job_config: Option<serde_json::Value>,
}

#[derive(Debug, Serialize)]
pub struct UdfInfo {
    pub id: Uuid,
    pub name: String,
    pub execution_mode: String,
    pub manifest: serde_json::Value,
    pub schedule: Option<String>,
    pub fuel_limit: i64,
    pub timeout_secs: i32,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

pub struct InitResult {
    pub loaded: usize,
    pub failed: usize,
    pub total: usize,
}

pub struct UdfRegistry {
    runtime: Arc<UdfRuntime>,
    udfs: RwLock<AHashMap<(Uuid, String), Arc<CompiledUdf>>>,
    db: Arc<DbPool>,
}

impl UdfRegistry {
    pub fn new(runtime: Arc<UdfRuntime>, db: Arc<DbPool>) -> Self {
        Self {
            runtime,
            udfs: RwLock::new(AHashMap::new()),
            db,
        }
    }

    pub fn runtime(&self) -> &UdfRuntime {
        &self.runtime
    }

    pub async fn register(
        &self,
        project_id: Uuid,
        name: &str,
        go_source: &str,
        mode: ExecutionMode,
        schedule: Option<&str>,
        fuel_limit: Option<u64>,
        timeout_secs: Option<u32>,
        job_config: Option<serde_json::Value>,
    ) -> Result<Manifest> {
        let source_owned = go_source.to_string();
        let compile_result = tokio::task::spawn_blocking(move || {
            let mut compiler = WasmCompiler::new();
            compiler
                .compile_source(&source_owned)
                .map_err(|e| anyhow::anyhow!("compilation failed: {}", e))
        })
        .await
        .context("compilation task panicked")??;

        let manifest = compile_result.manifest;
        let wasm_bytes = compile_result.wasm_bytes;

        let module = self
            .runtime
            .load_module(&wasm_bytes)
            .map_err(|e| {
                anyhow::anyhow!("failed to load compiled Wasm module: {:#}", e)
            })?;

        let source_hash = {
            let mut hasher = Sha256::new();
            hasher.update(go_source.as_bytes());
            hex::encode(hasher.finalize())
        };

        let manifest_json = serde_json::to_value(&manifest)?;
        let fuel = fuel_limit.unwrap_or(10_000_000) as i64;
        let timeout = timeout_secs.unwrap_or(300) as i32;

        let row = sqlx::query_as::<_, (Uuid,)>(
            r#"
            INSERT INTO warehouse_udfs (project_id, name, source_code, wasm_bytes, manifest, execution_mode, schedule, fuel_limit, timeout_secs, job_config)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            ON CONFLICT (project_id, name) DO UPDATE SET
                source_code = EXCLUDED.source_code,
                wasm_bytes = EXCLUDED.wasm_bytes,
                manifest = EXCLUDED.manifest,
                execution_mode = EXCLUDED.execution_mode,
                schedule = EXCLUDED.schedule,
                fuel_limit = EXCLUDED.fuel_limit,
                timeout_secs = EXCLUDED.timeout_secs,
                job_config = EXCLUDED.job_config,
                updated_at = NOW()
            RETURNING id
            "#,
        )
        .bind(project_id)
        .bind(name)
        .bind(go_source)
        .bind(&wasm_bytes)
        .bind(&manifest_json)
        .bind(mode.as_str())
        .bind(schedule)
        .bind(fuel)
        .bind(timeout)
        .bind(&job_config)
        .fetch_one(self.db.as_ref())
        .await
        .context("failed to store UDF in database")?;

        let udf_id = row.0;

        let compiled = Arc::new(CompiledUdf {
            id: udf_id,
            name: name.to_string(),
            module,
            manifest: manifest.clone(),
            execution_mode: mode,
            source_hash,
            fuel_limit: fuel as u64,
            timeout_secs: timeout as u32,
            schedule: schedule.map(String::from),
            job_config,
        });

        self.udfs
            .write()
            .insert((project_id, name.to_string()), compiled);

        Ok(manifest)
    }

    pub fn get(&self, project_id: Uuid, name: &str) -> Option<Arc<CompiledUdf>> {
        self.udfs.read().get(&(project_id, name.to_string())).cloned()
    }

    pub fn scheduled_jobs(&self) -> Vec<(Uuid, String, String)> {
        self.udfs
            .read()
            .iter()
            .filter_map(|((project_id, name), udf)| {
                if udf.execution_mode == ExecutionMode::Job {
                    udf.schedule.as_ref().map(|s| (*project_id, name.clone(), s.clone()))
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn reload(&self, project_id: Uuid, name: &str, compiled: Arc<CompiledUdf>) {
        self.udfs.write().insert((project_id, name.to_string()), compiled);
    }

    pub async fn list(&self, project_id: Uuid) -> Result<Vec<UdfInfo>> {
        let rows = sqlx::query_as::<_, (Uuid, String, String, serde_json::Value, Option<String>, i64, i32, chrono::DateTime<chrono::Utc>, chrono::DateTime<chrono::Utc>)>(
            r#"
            SELECT id, name, execution_mode, manifest, schedule, fuel_limit, timeout_secs, created_at, updated_at
            FROM warehouse_udfs
            WHERE project_id = $1
            ORDER BY name
            "#,
        )
        .bind(project_id)
        .fetch_all(self.db.as_ref())
        .await?;

        Ok(rows
            .into_iter()
            .map(|r| UdfInfo {
                id: r.0,
                name: r.1,
                execution_mode: r.2,
                manifest: r.3,
                schedule: r.4,
                fuel_limit: r.5,
                timeout_secs: r.6,
                created_at: r.7,
                updated_at: r.8,
            })
            .collect())
    }

    pub async fn get_info(&self, project_id: Uuid, name: &str) -> Result<Option<UdfInfo>> {
        let row = sqlx::query_as::<_, (Uuid, String, String, serde_json::Value, Option<String>, i64, i32, chrono::DateTime<chrono::Utc>, chrono::DateTime<chrono::Utc>)>(
            r#"
            SELECT id, name, execution_mode, manifest, schedule, fuel_limit, timeout_secs, created_at, updated_at
            FROM warehouse_udfs
            WHERE project_id = $1 AND name = $2
            "#,
        )
        .bind(project_id)
        .bind(name)
        .fetch_optional(self.db.as_ref())
        .await?;

        Ok(row.map(|r| UdfInfo {
            id: r.0,
            name: r.1,
            execution_mode: r.2,
            manifest: r.3,
            schedule: r.4,
            fuel_limit: r.5,
            timeout_secs: r.6,
            created_at: r.7,
            updated_at: r.8,
        }))
    }

    pub async fn delete(&self, project_id: Uuid, name: &str) -> Result<()> {
        sqlx::query("DELETE FROM warehouse_udfs WHERE project_id = $1 AND name = $2")
            .bind(project_id)
            .bind(name)
            .execute(self.db.as_ref())
            .await?;

        self.udfs.write().remove(&(project_id, name.to_string()));
        Ok(())
    }

    pub async fn initialize(&self) -> Result<InitResult> {
        let rows = sqlx::query_as::<_, (Uuid, Uuid, String, String, Vec<u8>, serde_json::Value, String, Option<String>, i64, i32, Option<serde_json::Value>)>(
            r#"
            SELECT id, project_id, name, source_code, wasm_bytes, manifest, execution_mode, schedule, fuel_limit, timeout_secs, job_config
            FROM warehouse_udfs
            "#,
        )
        .fetch_all(self.db.as_ref())
        .await?;

        let total = rows.len();
        let mut loaded = 0usize;
        let mut failed = 0usize;

        for row in rows {
            let (udf_id, project_id, name, source_code, wasm_bytes, manifest_json, mode_str, schedule, fuel_limit, timeout_secs, job_config) = row;

            let manifest: Manifest = match serde_json::from_value(manifest_json) {
                Ok(m) => m,
                Err(e) => {
                    tracing::warn!(udf_id = %udf_id, name = %name, error = %e, "Failed to deserialize UDF manifest");
                    failed += 1;
                    continue;
                }
            };

            let module = match self.runtime.load_module(&wasm_bytes) {
                Ok(m) => m,
                Err(e) => {
                    tracing::warn!(udf_id = %udf_id, name = %name, error = %e, "Failed to load UDF Wasm module");
                    failed += 1;
                    continue;
                }
            };

            let mode = match ExecutionMode::from_str(&mode_str) {
                Ok(m) => m,
                Err(e) => {
                    tracing::warn!(udf_id = %udf_id, name = %name, error = %e, "Unknown execution mode");
                    failed += 1;
                    continue;
                }
            };

            let source_hash = {
                let mut hasher = Sha256::new();
                hasher.update(source_code.as_bytes());
                hex::encode(hasher.finalize())
            };

            let compiled = Arc::new(CompiledUdf {
                id: udf_id,
                name: name.clone(),
                module,
                manifest,
                execution_mode: mode,
                source_hash,
                fuel_limit: fuel_limit as u64,
                timeout_secs: timeout_secs as u32,
                schedule,
                job_config,
            });

            self.udfs
                .write()
                .insert((project_id, name), compiled);
            loaded += 1;
        }

        Ok(InitResult {
            loaded,
            failed,
            total,
        })
    }
}
