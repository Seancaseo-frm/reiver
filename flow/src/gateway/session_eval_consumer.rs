//! Kafka consumer for session evaluation jobs. Receives idle-session
//! notifications produced by [`session_evaluator`](super::session_evaluator)
//! and performs:
//!
//! 1. **Classification** via moodeng (if the project has session labels)
//! 2. **Label insertion** into ClickHouse (`llm_message_labels`)
//! 3. **Aggregate fetching** (existing session summary + labels)
//! 4. **Profile matching** against user-defined session profiles
//! 5. **Persistence** of matched sessions to Postgres
//!
//! Consumer group `reiver-session-evaluator` guarantees each job is
//! processed by exactly one instance. Kafka key is `project_id` for
//! partition affinity.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use futures::{stream, StreamExt};
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::message::Message;
use serde::Deserialize;
use uuid::Uuid;

use reiver_core::clickhouse_db::ClickHousePool;
use reiver_core::db::DbPool;
use reiver_core::kafka::SessionEvalJobKafkaMessage;

use crate::api::llm_settings::SessionLabel;
use crate::api::session_profiles::{SessionAggregates, SessionProfile};
use crate::app_state::FlowState;
use crate::gateway::types::{
    ChatCompletionRequest, ChatMessage, MessageContent, MessageRole, ResponseFormat,
    ResponseFormatType,
};
use crate::moodeng::MoodengClient;
use crate::utils::escape_clickhouse_string;

const CONSUMER_GROUP: &str = "reiver-session-evaluator";
const REDIS_LABELS_TTL_SECS: i64 = 300; // 5 minutes

// Keep these settings aligned with deploy/gitops/infra/redpanda/create-topics-job.yaml.
const SESSION_EVAL_TOPIC_PARTITIONS: i32 = 3;
const SESSION_EVAL_TOPIC_REPLICATION_FACTOR: i32 = 1;
const SESSION_EVAL_TOPIC_RETENTION_MS: &str = "604800000";
const SESSION_EVAL_TOPIC_COMPRESSION: &str = "snappy";
const SESSION_EVAL_TOPIC_MAX_MESSAGE_BYTES: &str = "4194304";
const TOPIC_RETRY_INITIAL_DELAY: std::time::Duration =
    std::time::Duration::from_millis(250);
const TOPIC_RETRY_MAX_DELAY: std::time::Duration = std::time::Duration::from_secs(5);

struct KafkaConsumerContext;

impl rdkafka::ClientContext for KafkaConsumerContext {
    fn stats(&self, _stats: rdkafka::Statistics) {}
}

impl rdkafka::consumer::ConsumerContext for KafkaConsumerContext {}

/// Spawn the session evaluation consumer as a background task.
pub fn spawn(
    state: Arc<FlowState>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let topic = &state.config.kafka_session_eval_jobs_topic;
        let kafka_hosts = &state.config.kafka_hosts;
        let client_id = state.config.kafka_client_id.as_deref();

        // The GitOps topic job remains the primary cluster initializer. This
        // idempotent application-side check covers disposable/local stacks and
        // startup ordering where Flow can run before that job.
        if let Err(e) = ensure_session_eval_topic(kafka_hosts, topic, client_id).await {
            tracing::warn!(
                topic,
                error = %e,
                "Could not initialize session eval topic; consumer will keep retrying"
            );
        }

        let consumer = match create_consumer(kafka_hosts, topic, client_id) {
            Ok(c) => c,
            Err(e) => {
                tracing::error!(error = %e, "Failed to create session eval consumer");
                return;
            }
        };

        tracing::info!(topic, "Session eval consumer started");

        let mut message_stream = consumer.stream();
        let mut tasks = tokio::task::JoinSet::new();
        let mut missing_topic_retry_delay = TOPIC_RETRY_INITIAL_DELAY;
        const MAX_CONCURRENT: usize = 10;

        loop {
            // Back-pressure: when at capacity, drain one task before accepting more
            if tasks.len() >= MAX_CONCURRENT {
                if let Some(result) = tasks.join_next().await {
                    if let Err(e) = result {
                        tracing::warn!(error = %e, "Session eval task panicked");
                    }
                }
            }

            tokio::select! {
                message_opt = message_stream.next() => {
                    let Some(message_result) = message_opt else { break; };
                    match message_result {
                        Ok(m) => {
                            missing_topic_retry_delay = TOPIC_RETRY_INITIAL_DELAY;
                            let payload = match m.payload() {
                                Some(p) => p,
                                None => continue,
                            };
                            let job: SessionEvalJobKafkaMessage = match serde_json::from_slice(payload) {
                                Ok(j) => j,
                                Err(e) => {
                                    tracing::warn!(error = %e, "Failed to parse session eval job");
                                    continue;
                                }
                            };
                            let task_state = state.clone();
                            tasks.spawn(async move {
                                if let Err(e) = process_job(&task_state, &job).await {
                                    tracing::warn!(
                                        project_id = %job.project_id,
                                        session_id = %job.session_id,
                                        error = %e,
                                        "Session eval job failed"
                                    );
                                }
                            });
                        }
                        Err(e) => {
                            let missing_topic = is_unknown_topic_error(&e);
                            tracing::warn!(
                                error = %e,
                                missing_topic,
                                "Kafka consumer error"
                            );

                            if missing_topic {
                                if let Err(init_error) =
                                    ensure_session_eval_topic(kafka_hosts, topic, client_id).await
                                {
                                    tracing::warn!(
                                        topic,
                                        error = %init_error,
                                        "Session eval topic is still unavailable"
                                    );
                                }

                                // Re-subscribe to force an immediate metadata refresh. The
                                // stream and consumer instance stay alive; no process restart
                                // is required when the topic appears.
                                if let Err(subscribe_error) =
                                    consumer.subscribe(&[topic.as_str()])
                                {
                                    tracing::warn!(
                                        topic,
                                        error = %subscribe_error,
                                        "Failed to refresh session eval subscription"
                                    );
                                }

                                let shutdown_requested = tokio::select! {
                                    _ = tokio::time::sleep(missing_topic_retry_delay) => false,
                                    _ = shutdown.changed() => true,
                                };
                                if shutdown_requested {
                                    break;
                                }
                                missing_topic_retry_delay =
                                    (missing_topic_retry_delay * 2).min(TOPIC_RETRY_MAX_DELAY);
                            }
                        }
                    }
                }
                Some(result) = tasks.join_next(), if !tasks.is_empty() => {
                    if let Err(e) = result {
                        tracing::warn!(error = %e, "Session eval task panicked");
                    }
                }
                _ = shutdown.changed() => break,
            }
        }

        // Drain remaining tasks on shutdown
        while let Some(result) = tasks.join_next().await {
            if let Err(e) = result {
                tracing::warn!(error = %e, "Session eval task panicked during shutdown");
            }
        }
        tracing::info!("Session eval consumer shutting down");
    })
}

fn create_consumer(
    kafka_hosts: &str,
    topic: &str,
    client_id: Option<&str>,
) -> anyhow::Result<StreamConsumer<KafkaConsumerContext>> {
    let mut config = ClientConfig::new();
    config
        .set("bootstrap.servers", kafka_hosts)
        .set("group.id", CONSUMER_GROUP)
        .set("enable.auto.commit", "true")
        .set("auto.commit.interval.ms", "5000")
        .set("auto.offset.reset", "earliest")
        .set("session.timeout.ms", "30000")
        .set("enable.partition.eof", "false")
        .set("allow.auto.create.topics", "false")
        .set("topic.metadata.refresh.interval.ms", "5000")
        .set("topic.metadata.refresh.fast.interval.ms", "250")
        .set("retry.backoff.ms", "250")
        .set("retry.backoff.max.ms", "5000");

    if let Some(cid) = client_id {
        config.set("client.id", cid);
    }

    let consumer: StreamConsumer<KafkaConsumerContext> =
        config.create_with_context(KafkaConsumerContext)?;

    consumer.subscribe(&[topic])?;
    Ok(consumer)
}

fn session_eval_topic_definition(topic: &str) -> NewTopic<'_> {
    NewTopic::new(
        topic,
        SESSION_EVAL_TOPIC_PARTITIONS,
        TopicReplication::Fixed(SESSION_EVAL_TOPIC_REPLICATION_FACTOR),
    )
    .set("retention.ms", SESSION_EVAL_TOPIC_RETENTION_MS)
    .set("compression.type", SESSION_EVAL_TOPIC_COMPRESSION)
    .set("max.message.bytes", SESSION_EVAL_TOPIC_MAX_MESSAGE_BYTES)
}

async fn ensure_session_eval_topic(
    kafka_hosts: &str,
    topic: &str,
    client_id: Option<&str>,
) -> anyhow::Result<()> {
    let mut config = ClientConfig::new();
    config
        .set("bootstrap.servers", kafka_hosts)
        .set("socket.timeout.ms", "10000");

    if let Some(cid) = client_id {
        config.set("client.id", cid);
    }

    let admin: AdminClient<DefaultClientContext> = config.create()?;
    let definition = session_eval_topic_definition(topic);
    let options = AdminOptions::new()
        .request_timeout(Some(std::time::Duration::from_secs(10)))
        .operation_timeout(Some(std::time::Duration::from_secs(10)));

    let result = admin
        .create_topics([&definition], &options)
        .await?
        .into_iter()
        .next()
        .ok_or_else(|| anyhow::anyhow!("Kafka returned no topic creation result for {topic}"))?;

    match result {
        Ok(created_topic) => {
            tracing::info!(topic = %created_topic, "Session eval topic initialized");
            Ok(())
        }
        Err((_, RDKafkaErrorCode::TopicAlreadyExists)) => Ok(()),
        Err((returned_topic, code)) => Err(anyhow::anyhow!(
            "Kafka topic initialization failed for {returned_topic}: {code:?}"
        )),
    }
}

fn is_unknown_topic_error(error: &KafkaError) -> bool {
    error.rdkafka_error_code() == Some(RDKafkaErrorCode::UnknownTopicOrPartition)
}

/// Process a single session evaluation job.
async fn process_job(state: &FlowState, job: &SessionEvalJobKafkaMessage) -> anyhow::Result<()> {
    let project_id = Uuid::parse_str(&job.project_id)?;
    let session_id = &job.session_id;

    // 0. Check scan allowance — gate processing for free-tier users who've exceeded their limit
    let org_id = state.get_organization_id(project_id).await.ok().flatten();
    if let Some(oid) = org_id {
        if check_scan_allowance(&state.db, state.entitlements.as_ref(), oid).await {
            tracing::debug!(%project_id, %session_id, "Session eval scan limit reached, skipping");
            return Ok(());
        }
    }

    // 1. Load profiles -- we need them to know which labels to classify
    let profiles = load_project_profiles(&state.db, project_id).await?;
    if profiles.is_empty() {
        tracing::debug!(%project_id, %session_id, "No profiles configured, skipping evaluation");
        return Ok(());
    }

    // 2. Extract label names that are actually used in profile filters
    let needed_label_names = extract_label_names_from_profiles(&profiles);

    tracing::debug!(
        %project_id, %session_id,
        profile_count = profiles.len(),
        needed_labels = ?needed_label_names,
        "Evaluating session"
    );

    // 3. Classify per-message (only if profiles reference labels)
    let all_labels = if !needed_label_names.is_empty() {
        let taxonomy = load_session_labels(state, project_id).await?;
        let relevant: Vec<SessionLabel> = taxonomy
            .into_iter()
            .filter(|l| needed_label_names.contains(l.name.as_str()))
            .collect();

        if relevant.is_empty() {
            tracing::debug!(
                %project_id, %session_id,
                needed = ?needed_label_names,
                "Profile filters reference labels not in taxonomy, skipping classification"
            );
            vec![]
        } else {
            classify_and_insert_labels(state, project_id, session_id, &relevant).await?
        }
    } else {
        tracing::debug!(%project_id, %session_id, "No label filters in profiles, skipping classification");
        vec![]
    };

    // Emit scan metering event (one per evaluated session, regardless of match outcome)
    if let Some(org_id) = org_id {
        let scan_key = format!("scan-{}-{}", project_id, session_id);
        let _ = sqlx::query(
            "INSERT INTO mcp_credit_log (organization_id, project_id, tool_name, credits, idempotency_key) VALUES ($1, $2, 'session_scan', 1, $3) ON CONFLICT (idempotency_key) DO NOTHING"
        )
        .bind(org_id)
        .bind(project_id)
        .bind(&scan_key)
        .execute(&*state.db)
        .await;
        state.meter_service.record_scan(org_id, scan_key);
    }

    // 4. Fetch aggregates (including labels)
    let summary =
        fetch_session_summary(&state.clickhouse, project_id, session_id, &all_labels).await?;

    // 5. Match profiles
    let matched: Vec<(&Uuid, &str)> = profiles
        .iter()
        .filter(|p| p.matches(&summary.agg))
        .map(|p| (&p.id, p.name.as_str()))
        .collect();

    if matched.is_empty() {
        tracing::info!(
            %project_id, %session_id,
            profile_count = profiles.len(),
            labels = ?all_labels,
            "No profiles matched"
        );
        return Ok(());
    }

    let matched_names: Vec<&str> = matched.iter().map(|(_, n)| *n).collect();
    let matched_ids: Vec<&Uuid> = matched.iter().map(|(id, _)| *id).collect();

    tracing::info!(
        %project_id,
        %session_id,
        matched_profiles = ?matched_names,
        labels = ?all_labels,
        "Profiles matched — saving session"
    );

    // 6. Copy content + save summary to Postgres
    if let Err(e) =
        copy_session_content_to_postgres(&state.db, &state.clickhouse, project_id, session_id).await
    {
        tracing::warn!(%project_id, %session_id, error = %e, "Failed to copy session content to Postgres");
    }

    if let Err(e) = save_session_summary(&state.db, project_id, session_id, &summary).await {
        tracing::warn!(%project_id, %session_id, error = %e, "Failed to save session summary to Postgres");
    }

    for profile_id in &matched_ids {
        if let Err(e) = sqlx::query(
            r#"
            INSERT INTO session_profile_matches (id, project_id, session_id, profile_id, matched_at)
            VALUES ($1, $2, $3, $4, NOW())
            ON CONFLICT (project_id, session_id, profile_id) DO NOTHING
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(project_id)
        .bind(session_id)
        .bind(*profile_id)
        .execute(state.db.as_ref())
        .await
        {
            tracing::warn!(
                %project_id,
                %session_id,
                profile_id = %profile_id,
                error = %e,
                "Failed to insert session profile match"
            );
        }
    }

    Ok(())
}

/// Extract label names that are referenced in `labels.names` filters across all profiles.
fn extract_label_names_from_profiles(
    profiles: &[SessionProfile],
) -> std::collections::HashSet<&str> {
    let mut names = std::collections::HashSet::new();
    for profile in profiles {
        for filter in &profile.filters {
            if filter.field == "labels.names" {
                if let Some(serde_json::Value::String(ref v)) = filter.value {
                    names.insert(v.as_str());
                }
            }
        }
    }
    names
}

// ============================================================================
// Classification
// ============================================================================

/// Classify each message in a session individually, insert per-message labels
/// into ClickHouse, and return the union of all labels across the session.
async fn classify_and_insert_labels(
    state: &FlowState,
    project_id: Uuid,
    session_id: &str,
    labels: &[SessionLabel],
) -> anyhow::Result<Vec<String>> {
    let messages = fetch_session_messages(&state.clickhouse, project_id, session_id).await?;
    if messages.is_empty() {
        tracing::debug!(%project_id, %session_id, "No messages found for session, skipping classification");
        return Ok(vec![]);
    }

    let valid_names: std::collections::HashSet<&str> =
        labels.iter().map(|l| l.name.as_str()).collect();

    let labels_text = format_labels_for_prompt(labels);
    let moodeng = MoodengClient::new(state, project_id);

    let classify_inputs: Vec<(String, String)> = messages
        .iter()
        .filter_map(|msg| {
            let content = format_message_content(&msg.request_messages, &msg.response_content);
            if content.is_empty() {
                None
            } else {
                Some((msg.request_id.clone(), content))
            }
        })
        .collect();

    let results: Vec<(String, Vec<String>)> = stream::iter(classify_inputs)
        .map(|(request_id, content)| {
            let moodeng = &moodeng;
            let labels_text = &labels_text;
            let valid_names = &valid_names;
            async move {
                let msg_labels = classify_single_message(
                    moodeng,
                    labels_text,
                    &content,
                    project_id,
                    session_id,
                    &request_id,
                    valid_names,
                )
                .await;
                (request_id, msg_labels)
            }
        })
        .buffer_unordered(5)
        .collect()
        .await;

    let mut all_labels = std::collections::HashSet::new();
    let mut insert_rows: Vec<(String, Vec<String>)> = Vec::new();
    for (request_id, msg_labels) in results {
        if !msg_labels.is_empty() {
            for l in &msg_labels {
                all_labels.insert(l.clone());
            }
            insert_rows.push((request_id, msg_labels));
        }
    }

    // Batch insert all per-message labels into ClickHouse
    if !insert_rows.is_empty() {
        insert_per_message_labels(
            &state.clickhouse,
            &project_id.to_string(),
            session_id,
            &insert_rows,
        )
        .await?;
    }

    let result: Vec<String> = all_labels.into_iter().collect();
    tracing::info!(
        %project_id,
        %session_id,
        labels = ?result,
        message_count = messages.len(),
        labeled_count = insert_rows.len(),
        "Session messages classified"
    );

    Ok(result)
}

fn format_labels_for_prompt(labels: &[SessionLabel]) -> String {
    labels
        .iter()
        .map(|l| {
            if l.definition.trim().is_empty() {
                format!(
                    "- {} (use your best judgement based on the label name)",
                    l.name
                )
            } else {
                format!("- {}: {}", l.name, l.definition)
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn format_message_content(request_messages: &str, response_content: &str) -> String {
    let mut parts = Vec::new();

    if !request_messages.is_empty() {
        if let Ok(messages) = serde_json::from_str::<Vec<serde_json::Value>>(request_messages) {
            for msg in &messages {
                let role = msg.get("role").and_then(|r| r.as_str()).unwrap_or("");
                if role == "system" {
                    continue;
                }
                let content = msg.get("content").and_then(|c| c.as_str()).unwrap_or("");
                if !content.is_empty() {
                    parts.push(format!("[{}]\n{}", role, content));
                }
            }
        } else if !request_messages.trim().is_empty() {
            parts.push(format!("[Request]\n{}", request_messages));
        }
    }

    if !response_content.is_empty() {
        parts.push(format!("[Response]\n{}", response_content));
    }
    parts.join("\n\n")
}

/// Classify a single message against the label taxonomy via moodeng.
async fn classify_single_message(
    moodeng: &MoodengClient<'_>,
    labels_text: &str,
    message_content: &str,
    project_id: Uuid,
    session_id: &str,
    request_id: &str,
    valid_names: &std::collections::HashSet<&str>,
) -> Vec<String> {
    let mut prompt_variables = std::collections::HashMap::new();
    prompt_variables.insert(
        "labels".to_string(),
        serde_json::Value::String(labels_text.to_string()),
    );

    let request = ChatCompletionRequest {
        model: String::new(),
        messages: vec![ChatMessage {
            role: MessageRole::User,
            content: Some(MessageContent::Text(message_content.to_string())),
            name: None,
            tool_calls: None,
            tool_call_id: None,
            reasoning_content: None,
        }],
        temperature: Some(0.0),
        max_tokens: Some(200),
        stream: Some(false),
        prompt_config: Some("moodeng-session-classifier".to_string()),
        prompt_variables: Some(prompt_variables),
        response_format: Some(ResponseFormat {
            format_type: ResponseFormatType::JsonObject,
        }),
        ..Default::default()
    };

    let result = match moodeng.call_llm(&request, None).await {
        Ok(r) => r,
        Err(e) => {
            let err_msg = e.to_string();
            let span = tracing::error_span!(
                "session_eval.classify_message",
                %project_id, %session_id, %request_id,
                error.message = %err_msg,
                otel.status_code = "ERROR",
                otel.status_message = %err_msg,
            );
            let _guard = span.enter();
            tracing::error!(error = %err_msg, "Classification call failed");
            return vec![];
        }
    };

    #[derive(Deserialize)]
    struct ClassificationResponse {
        labels: Vec<String>,
    }

    match serde_json::from_str::<ClassificationResponse>(result.content.trim()) {
        Ok(parsed) => {
            let valid: Vec<String> = parsed
                .labels
                .into_iter()
                .filter(|l| valid_names.contains(l.as_str()))
                .collect();
            tracing::debug!(
                %project_id, %session_id, %request_id,
                labels = ?valid,
                "Message classified"
            );
            valid
        }
        Err(e) => {
            let err_msg = e.to_string();
            let span = tracing::error_span!(
                "session_eval.classify_message",
                %project_id, %session_id, %request_id,
                error.message = %err_msg,
                raw_content = %result.content,
                otel.status_code = "ERROR",
                otel.status_message = %err_msg,
            );
            let _guard = span.enter();
            tracing::error!(error = %err_msg, "Failed to parse classification JSON");
            vec![]
        }
    }
}

/// Load session labels for a project. Checks Redis cache first, then Postgres.
async fn load_session_labels(
    state: &FlowState,
    project_id: Uuid,
) -> anyhow::Result<Vec<SessionLabel>> {
    let cache_key = format!("session_labels:{}", project_id);

    // Try Redis cache
    if let Ok(mut conn) = state.redis.get().await {
        if let Ok(Some(cached)) = redis::cmd("GET")
            .arg(&cache_key)
            .query_async::<Option<String>>(&mut *conn)
            .await
        {
            if let Ok(labels) = serde_json::from_str::<Vec<SessionLabel>>(&cached) {
                tracing::debug!(%project_id, label_count = labels.len(), "Session labels loaded from Redis cache");
                return Ok(labels);
            }
        }
    }

    // Load from Postgres
    #[derive(sqlx::FromRow)]
    struct Row {
        value: String,
    }

    let row = sqlx::query_as::<_, Row>(
        "SELECT value FROM project_settings WHERE project_id = $1 AND key = 'gateway_session_labels'",
    )
    .bind(project_id)
    .fetch_optional(state.db.as_ref())
    .await?;

    let labels: Vec<SessionLabel> = match row {
        Some(r) if !r.value.is_empty() => serde_json::from_str(&r.value).unwrap_or_default(),
        _ => vec![],
    };

    tracing::debug!(%project_id, label_count = labels.len(), "Session labels loaded from Postgres");

    // Cache in Redis
    if let Ok(mut conn) = state.redis.get().await {
        let json = serde_json::to_string(&labels).unwrap_or_else(|_| "[]".to_string());
        let _ = redis::cmd("SET")
            .arg(&cache_key)
            .arg(&json)
            .arg("EX")
            .arg(REDIS_LABELS_TTL_SECS)
            .query_async::<String>(&mut *conn)
            .await;
    }

    Ok(labels)
}

/// Fetch individual messages from ClickHouse for per-message classification.
#[derive(Debug, clickhouse::Row, serde::Deserialize)]
struct SessionMessage {
    request_id: String,
    request_messages: String,
    response_content: String,
}

async fn fetch_session_messages(
    clickhouse: &ClickHousePool,
    project_id: Uuid,
    session_id: &str,
) -> anyhow::Result<Vec<SessionMessage>> {
    let query = format!(
        r#"
        SELECT
            request_id,
            request_messages,
            response_content
        FROM reiver.llm_requests
        WHERE project_id = '{}'
            AND session_id = '{}'
            AND (request_messages != '' OR response_content != '')
        ORDER BY timestamp ASC
        "#,
        project_id,
        escape_clickhouse_string(session_id),
    );

    let rows: Vec<SessionMessage> = clickhouse
        .query(&query)
        .fetch_all()
        .await
        .map_err(|e| anyhow::anyhow!("ClickHouse messages query error: {}", e))?;

    Ok(rows)
}

/// Batch insert per-message labels into ClickHouse. Each entry in `rows` is
/// a (request_id, labels) pair for a single message.
async fn insert_per_message_labels(
    clickhouse: &ClickHousePool,
    project_id: &str,
    session_id: &str,
    rows: &[(String, Vec<String>)],
) -> anyhow::Result<()> {
    if rows.is_empty() {
        return Ok(());
    }

    let mut values = Vec::with_capacity(rows.len());
    for (request_id, labels) in rows {
        let labels_sql: Vec<String> = labels
            .iter()
            .map(|l| format!("'{}'", escape_clickhouse_string(l)))
            .collect();
        let labels_array = format!("[{}]", labels_sql.join(", "));
        values.push(format!(
            "('{}', '{}', '{}', {})",
            escape_clickhouse_string(project_id),
            escape_clickhouse_string(session_id),
            escape_clickhouse_string(request_id),
            labels_array,
        ));
    }

    let insert_query = format!(
        "INSERT INTO reiver.llm_message_labels (project_id, session_id, request_id, labels) VALUES {}",
        values.join(", ")
    );

    clickhouse
        .query(&insert_query)
        .execute()
        .await
        .map_err(|e| anyhow::anyhow!("ClickHouse labels insert error: {}", e))?;

    tracing::debug!(
        project_id,
        session_id,
        row_count = rows.len(),
        "Inserted session labels into ClickHouse"
    );

    Ok(())
}

// ============================================================================
// Session aggregation (reused from old session_evaluator, extended with labels)
// ============================================================================

struct SessionSummary {
    agg: SessionAggregates,
    session_name: String,
    user_id: String,
    first_request_time: DateTime<Utc>,
    last_request_time: DateTime<Utc>,
    request_count: u64,
    total_input_tokens: u64,
    total_output_tokens: u64,
}

/// Fetch aggregated session data from ClickHouse, including labels.
async fn fetch_session_summary(
    clickhouse: &ClickHousePool,
    project_id: Uuid,
    session_id: &str,
    labels: &[String],
) -> anyhow::Result<SessionSummary> {
    let query = format!(
        r#"
        SELECT
            anyLast(session_name) as session_name,
            anyLast(user_id) as user_id,
            min(timestamp) as first_request_time,
            max(timestamp) as last_request_time,
            count() as request_count,
            sum(input_tokens) as total_input_tokens,
            sum(output_tokens) as total_output_tokens,
            countIf(status_code = 'error') as error_count,
            toUInt32(avg(duration_ms)) as avg_latency_ms,
            max(duration_ms) as max_latency_ms,
            sum(cost_usd) as total_cost,
            if(count() > 0, sum(cost_usd) / count(), 0) as avg_cost_per_call,
            groupUniqArray(gen_ai_system) as providers,
            groupUniqArray(gen_ai_request_model) as models,
            groupUniqArray(prompt_config_id) as prompt_config_ids,
            countIf(fallback_used = 1) as fallback_count,
            countIf(length(guardrail_violations) > 0) as guardrail_count,
            sum(tool_call_count) as total_tool_call_count,
            groupUniqArrayArray(tool_names) as unique_tool_names
        FROM reiver.llm_requests
        WHERE project_id = '{}'
            AND session_id = '{}'
        "#,
        project_id,
        escape_clickhouse_string(session_id),
    );

    #[derive(Debug, clickhouse::Row, serde::Deserialize)]
    struct Row {
        session_name: String,
        user_id: String,
        #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
        first_request_time: DateTime<Utc>,
        #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
        last_request_time: DateTime<Utc>,
        request_count: u64,
        total_input_tokens: u64,
        total_output_tokens: u64,
        error_count: u64,
        avg_latency_ms: u32,
        max_latency_ms: u32,
        total_cost: f64,
        avg_cost_per_call: f64,
        providers: Vec<String>,
        models: Vec<String>,
        prompt_config_ids: Vec<String>,
        fallback_count: u64,
        guardrail_count: u64,
        total_tool_call_count: u64,
        unique_tool_names: Vec<String>,
    }

    let row: Row = clickhouse
        .query(&query)
        .fetch_one()
        .await
        .map_err(|e| anyhow::anyhow!("ClickHouse aggregate query error: {}", e))?;

    Ok(SessionSummary {
        session_name: row.session_name,
        user_id: row.user_id,
        first_request_time: row.first_request_time,
        last_request_time: row.last_request_time,
        request_count: row.request_count,
        total_input_tokens: row.total_input_tokens,
        total_output_tokens: row.total_output_tokens,
        agg: SessionAggregates {
            error_count: row.error_count,
            avg_latency_ms: row.avg_latency_ms,
            max_latency_ms: row.max_latency_ms,
            total_cost: row.total_cost,
            avg_cost_per_call: row.avg_cost_per_call,
            providers: row.providers,
            models: row.models,
            prompt_config_ids: row.prompt_config_ids,
            fallback_count: row.fallback_count,
            guardrail_count: row.guardrail_count,
            tool_call_count: row.total_tool_call_count,
            tool_names: row.unique_tool_names,
            labels: labels.to_vec(),
        },
    })
}

// ============================================================================
// Profile loading
// ============================================================================

async fn load_project_profiles(
    db: &DbPool,
    project_id: Uuid,
) -> anyhow::Result<Vec<SessionProfile>> {
    #[derive(sqlx::FromRow)]
    struct Row {
        value: String,
    }

    let row = sqlx::query_as::<_, Row>(
        "SELECT value FROM project_settings WHERE project_id = $1 AND key = 'gateway_session_profiles'",
    )
    .bind(project_id)
    .fetch_optional(db)
    .await?;

    match row {
        Some(r) if !r.value.is_empty() => {
            let mut profiles: Vec<SessionProfile> =
                serde_json::from_str(&r.value).unwrap_or_default();
            crate::api::session_profiles::migrate_profiles(&mut profiles);
            Ok(profiles)
        }
        _ => Ok(vec![]),
    }
}

// ============================================================================
// Postgres persistence (reused from old session_evaluator)
// ============================================================================

async fn save_session_summary(
    db: &DbPool,
    project_id: Uuid,
    session_id: &str,
    summary: &SessionSummary,
) -> anyhow::Result<()> {
    let models: Vec<&str> = summary.agg.models.iter().map(|s| s.as_str()).collect();
    let labels: Vec<&str> = summary.agg.labels.iter().map(|s| s.as_str()).collect();
    sqlx::query(
        r#"
        INSERT INTO saved_sessions
            (project_id, session_id, session_name, user_id,
             first_request_time, last_request_time, request_count,
             total_input_tokens, total_output_tokens, total_cost_usd,
             avg_latency_ms, error_count, models, fallback_count, guardrail_count, labels)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
        ON CONFLICT (project_id, session_id) DO NOTHING
        "#,
    )
    .bind(project_id)
    .bind(session_id)
    .bind(&summary.session_name)
    .bind(&summary.user_id)
    .bind(summary.first_request_time)
    .bind(summary.last_request_time)
    .bind(summary.request_count as i32)
    .bind(summary.total_input_tokens as i64)
    .bind(summary.total_output_tokens as i64)
    .bind(summary.agg.total_cost)
    .bind(summary.agg.avg_latency_ms as i32)
    .bind(summary.agg.error_count as i32)
    .bind(&models)
    .bind(summary.agg.fallback_count as i32)
    .bind(summary.agg.guardrail_count as i32)
    .bind(&labels)
    .execute(db)
    .await?;

    tracing::debug!(
        %project_id,
        %session_id,
        request_count = summary.request_count,
        labels = ?labels,
        "Saved session summary to Postgres"
    );

    Ok(())
}

async fn copy_session_content_to_postgres(
    db: &DbPool,
    clickhouse: &ClickHousePool,
    project_id: Uuid,
    session_id: &str,
) -> anyhow::Result<()> {
    let query = format!(
        r#"
        SELECT
            request_id,
            request_messages,
            response_content,
            gen_ai_request_model,
            gen_ai_system,
            input_tokens,
            output_tokens,
            cost_usd,
            duration_ms,
            status_code,
            timestamp,
            fallback_used,
            original_model,
            retry_count,
            guardrail_violations,
            temperature,
            top_p,
            max_tokens,
            frequency_penalty,
            presence_penalty,
            is_platform_key
        FROM reiver.llm_requests
        WHERE project_id = '{}'
            AND session_id = '{}'
            AND (request_messages != '' OR response_content != '')
        ORDER BY timestamp ASC
        "#,
        project_id,
        escape_clickhouse_string(session_id),
    );

    #[derive(Debug, clickhouse::Row, serde::Deserialize)]
    struct ContentRow {
        request_id: String,
        request_messages: String,
        response_content: String,
        gen_ai_request_model: String,
        gen_ai_system: String,
        input_tokens: u32,
        output_tokens: u32,
        cost_usd: f64,
        duration_ms: u32,
        status_code: String,
        #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
        timestamp: DateTime<Utc>,
        fallback_used: u8,
        original_model: String,
        retry_count: u32,
        guardrail_violations: Vec<String>,
        temperature: f32,
        top_p: f32,
        max_tokens: u32,
        frequency_penalty: f32,
        presence_penalty: f32,
        is_platform_key: u8,
    }

    let rows: Vec<ContentRow> = clickhouse
        .query(&query)
        .fetch_all()
        .await
        .map_err(|e| anyhow::anyhow!("ClickHouse content query error: {}", e))?;

    if rows.is_empty() {
        return Ok(());
    }

    tracing::debug!(
        project_id = %project_id,
        session_id = %session_id,
        row_count = rows.len(),
        "Copying session content to Postgres"
    );

    let mut qb: sqlx::QueryBuilder<sqlx::Postgres> = sqlx::QueryBuilder::new(
        "INSERT INTO session_request_content \
         (project_id, session_id, request_id, request_messages, response_content, \
          gen_ai_request_model, gen_ai_system, input_tokens, output_tokens, \
          cost_usd, duration_ms, status_code, timestamp, \
          fallback_used, original_model, retry_count, guardrail_violations, \
          temperature, top_p, max_tokens, frequency_penalty, presence_penalty, \
          is_platform_key) ",
    );
    qb.push_values(rows.iter(), |mut b, row| {
        b.push_bind(project_id)
            .push_bind(session_id.to_owned())
            .push_bind(&row.request_id)
            .push_bind(&row.request_messages)
            .push_bind(&row.response_content)
            .push_bind(&row.gen_ai_request_model)
            .push_bind(&row.gen_ai_system)
            .push_bind(row.input_tokens as i32)
            .push_bind(row.output_tokens as i32)
            .push_bind(row.cost_usd)
            .push_bind(row.duration_ms as i32)
            .push_bind(&row.status_code)
            .push_bind(row.timestamp)
            .push_bind(row.fallback_used != 0)
            .push_bind(&row.original_model)
            .push_bind(row.retry_count as i32)
            .push_bind(&row.guardrail_violations)
            .push_bind(if row.temperature != 0.0 {
                Some(row.temperature)
            } else {
                None::<f32>
            })
            .push_bind(if row.top_p != 0.0 {
                Some(row.top_p)
            } else {
                None::<f32>
            })
            .push_bind(if row.max_tokens != 0 {
                Some(row.max_tokens as i32)
            } else {
                None::<i32>
            })
            .push_bind(if row.frequency_penalty != 0.0 {
                Some(row.frequency_penalty)
            } else {
                None::<f32>
            })
            .push_bind(if row.presence_penalty != 0.0 {
                Some(row.presence_penalty)
            } else {
                None::<f32>
            })
            .push_bind(row.is_platform_key != 0);
    });
    qb.push(" ON CONFLICT (project_id, session_id, request_id) DO NOTHING");
    qb.build().execute(db).await?;

    Ok(())
}

/// Returns true if the org has exceeded its scan allotment (free tier only).
/// Paid tiers always return false (Stripe handles overage billing).
async fn check_scan_allowance(
    db: &DbPool,
    entitlements: &dyn reiver_core::entitlements::EntitlementChecker,
    org_id: Uuid,
) -> bool {
    // Get the limit from the cached entitlements service
    let limit = match entitlements.get_config(org_id).await {
        Ok(tier) => tier.config.prompt_hub.session_evals_included,
        Err(_) => -1,
    };

    if limit < 0 {
        return false;
    }

    let has_subscription: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM stripe_subscriptions WHERE organization_id = $1 AND status IN ('active', 'trialing'))"
    )
    .bind(org_id)
    .fetch_one(db)
    .await
    .unwrap_or(false);

    if has_subscription {
        return false;
    }

    let billing_start = {
        use chrono::Datelike;
        let now = chrono::Utc::now();
        now.with_day(1)
            .unwrap_or(now)
            .date_naive()
            .and_hms_opt(0, 0, 0)
            .unwrap()
            .and_utc()
    };

    let used: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM mcp_credit_log WHERE organization_id = $1 AND tool_name = 'session_scan' AND created_at >= $2"
    )
    .bind(org_id)
    .bind(billing_start)
    .fetch_one(db)
    .await
    .unwrap_or(0);

    used >= limit
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt as _;
    use rdkafka::mocking::MockCluster;
    use rdkafka::producer::{FutureProducer, FutureRecord};

    #[test]
    fn session_eval_topic_definition_matches_deployment_convention() {
        let definition = session_eval_topic_definition("reiver.session.eval.jobs");

        assert_eq!(definition.num_partitions, SESSION_EVAL_TOPIC_PARTITIONS);
        assert!(matches!(
            &definition.replication,
            TopicReplication::Fixed(value) if *value == SESSION_EVAL_TOPIC_REPLICATION_FACTOR
        ));
        assert_eq!(
            definition.config,
            vec![
                ("retention.ms", SESSION_EVAL_TOPIC_RETENTION_MS),
                ("compression.type", SESSION_EVAL_TOPIC_COMPRESSION),
                ("max.message.bytes", SESSION_EVAL_TOPIC_MAX_MESSAGE_BYTES),
            ]
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn consumer_recovers_when_flow_starts_before_topic_and_delivers_ended_session() {
        let cluster = MockCluster::new(1).expect("mock Kafka cluster must start");
        let kafka_hosts = cluster.bootstrap_servers();
        let topic = format!("reiver.session.eval.jobs.{}", Uuid::new_v4());
        let consumer = create_consumer(&kafka_hosts, &topic, Some("session-eval-recovery-test"))
            .expect("consumer must start before the topic exists");
        let mut message_stream = consumer.stream();

        // Drive metadata discovery while the topic is absent and prove the
        // exact transient failure that previously stranded the consumer.
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                match message_stream.next().await {
                    Some(Err(error)) if is_unknown_topic_error(&error) => break,
                    Some(Err(_)) => continue,
                    Some(Ok(_)) => panic!("received a message before the topic existed"),
                    None => panic!("consumer stream ended while the topic was absent"),
                }
            }
        })
        .await
        .expect("consumer did not report UnknownTopicOrPartition");

        cluster
            .create_topic(&topic, SESSION_EVAL_TOPIC_PARTITIONS, 1)
            .expect("topic must be created after the consumer starts");
        consumer
            .subscribe(&[topic.as_str()])
            .expect("consumer must refresh its subscription without a restart");

        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", &kafka_hosts)
            .create()
            .expect("producer must connect to mock Kafka");

        let expected = SessionEvalJobKafkaMessage {
            project_id: Uuid::new_v4().to_string(),
            session_id: "ended-session-queryable-after-recovery".to_string(),
            enqueued_at: Utc::now().to_rfc3339(),
        };
        let payload = serde_json::to_string(&expected).expect("job must serialize");

        producer
            .send(
                FutureRecord::to(&topic)
                    .key(&expected.project_id)
                    .payload(&payload),
                std::time::Duration::from_secs(5),
            )
            .await
            .expect("ended-session job must be published");

        let delivered = tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                match message_stream.next().await {
                    Some(Ok(message)) => break message,
                    Some(Err(error)) if is_unknown_topic_error(&error) => continue,
                    Some(Err(error)) => {
                        panic!("unexpected Kafka error after topic creation: {error}")
                    }
                    None => panic!("consumer stream ended before recovery"),
                }
            }
        })
        .await
        .expect("consumer did not recover after the topic appeared");

        // In production, delivery enters the unchanged process_job path,
        // which writes saved_sessions — the table queried by Flow and MCP.
        let actual: SessionEvalJobKafkaMessage =
            serde_json::from_slice(delivered.payload().expect("job must have a payload"))
                .expect("job payload must parse");
        assert_eq!(actual.project_id, expected.project_id);
        assert_eq!(actual.session_id, expected.session_id);
        assert_eq!(actual.enqueued_at, expected.enqueued_at);
    }
}
