use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use arrow::record_batch::RecordBatch;
use wasmtime::Module;

use gno_rs::wasm::runtime::{HostState, UdfRuntime};
use gno_rs::wasm::udf::TableDescriptor;

use super::arrow_bridge::ArrowWasmBridge;

pub struct UdfWorkerPool {
    rayon_pool: rayon::ThreadPool,
    runtime: Arc<UdfRuntime>,
    semaphore: Arc<tokio::sync::Semaphore>,
    max_instance_memory: usize,
}

impl UdfWorkerPool {
    pub fn new(
        pool_size: usize,
        max_concurrent: usize,
        max_instance_memory: usize,
        runtime: Arc<UdfRuntime>,
    ) -> Self {
        let rayon_pool = rayon::ThreadPoolBuilder::new()
            .num_threads(pool_size)
            .thread_name(|i| format!("udf-worker-{}", i))
            .build()
            .expect("failed to build UDF rayon thread pool");

        Self {
            rayon_pool,
            runtime,
            semaphore: Arc::new(tokio::sync::Semaphore::new(max_concurrent)),
            max_instance_memory,
        }
    }

    pub async fn process_batch(
        &self,
        module: &Arc<Module>,
        batch: RecordBatch,
        input_schema: &TableDescriptor,
        output_schema: &TableDescriptor,
        fuel_limit: u64,
        udf_func_name: &str,
    ) -> Result<RecordBatch> {
        self.process_batch_with_timeout(
            module,
            batch,
            input_schema,
            output_schema,
            fuel_limit,
            udf_func_name,
            300,
        )
        .await
    }

    pub async fn process_batch_with_timeout(
        &self,
        module: &Arc<Module>,
        batch: RecordBatch,
        input_schema: &TableDescriptor,
        output_schema: &TableDescriptor,
        fuel_limit: u64,
        udf_func_name: &str,
        timeout_secs: u32,
    ) -> Result<RecordBatch> {
        self.process_batch_with_config(
            module,
            batch,
            input_schema,
            output_schema,
            fuel_limit,
            udf_func_name,
            timeout_secs,
            None,
        )
        .await
    }

    /// Process a batch through a UDF with optional config params injected into
    /// the Wasm guest. Config params are accessible in Go via `os.Getenv()`.
    pub async fn process_batch_with_config(
        &self,
        module: &Arc<Module>,
        batch: RecordBatch,
        input_schema: &TableDescriptor,
        output_schema: &TableDescriptor,
        fuel_limit: u64,
        udf_func_name: &str,
        timeout_secs: u32,
        config_params: Option<&HashMap<String, String>>,
    ) -> Result<RecordBatch> {
        let _permit = self
            .semaphore
            .acquire()
            .await
            .map_err(|_| anyhow::anyhow!("UDF semaphore closed"))?;

        let runtime = self.runtime.clone();
        let module = module.clone();
        let input_schema = input_schema.clone();
        let output_schema = output_schema.clone();
        let func_name = format!("__udf_{}", udf_func_name);
        let config = config_params.cloned().unwrap_or_default();

        let (tx, rx) = tokio::sync::oneshot::channel();

        self.rayon_pool.spawn(move || {
            let result = (|| -> Result<RecordBatch> {
                let mut host_state = HostState::new();
                for (k, v) in &config {
                    host_state = host_state.with_config(k, v);
                }
                let mut store = runtime.create_store(host_state, fuel_limit)?;
                let instance = runtime.instantiate(&mut store, &module)?;

                let input_ptr = ArrowWasmBridge::write_batch_to_wasm(
                    &mut store,
                    &instance,
                    &batch,
                    &input_schema,
                )?;

                let transform_fn = instance
                    .get_typed_func::<i32, i32>(&mut store, &func_name)
                    .map_err(|e| anyhow::anyhow!("UDF export '{}' not found: {}", func_name, e))?;

                let output_ptr = transform_fn.call(&mut store, input_ptr)?;

                ArrowWasmBridge::read_batch_from_wasm(
                    &mut store,
                    &instance,
                    output_ptr,
                    &output_schema,
                )
            })();

            let _ = tx.send(result);
        });

        let timeout_duration = Duration::from_secs(timeout_secs.max(1) as u64);
        match tokio::time::timeout(timeout_duration, rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(anyhow::anyhow!("UDF worker channel closed")),
            Err(_) => Err(anyhow::anyhow!(
                "UDF execution timed out after {} seconds",
                timeout_secs
            )),
        }
    }

    pub fn max_instance_memory(&self) -> usize {
        self.max_instance_memory
    }
}
