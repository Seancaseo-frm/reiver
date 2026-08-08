-- Phase 6: Seed default auto-investigate event subscriptions for projects
-- that already have gateway_auto_investigate enabled.
--
-- Once this event subscription is confirmed working, the hardcoded
-- trigger_alert_investigation / trigger_exception_investigation calls
-- in watch can be removed.

INSERT INTO event_subscriptions (
    project_id, name, enabled, event_types,
    action_type, action_config, cooldown_seconds, max_retries
)
SELECT
    ps.project_id,
    'Auto-Investigate (migrated)',
    true,
    ARRAY['alert_fired', 'exception_group_created', 'exception_group_regressed'],
    'agent_task',
    jsonb_build_object('prompt_template', 'Investigate: {{trigger_summary}}'),
    300,  -- 5-minute cooldown to avoid flooding
    2
FROM project_settings ps
WHERE ps.key = 'gateway_auto_investigate'
  AND ps.value = 'true'
  AND NOT EXISTS (
    SELECT 1 FROM event_subscriptions es
    WHERE es.project_id = ps.project_id
      AND es.action_type = 'agent_task'
      AND es.name = 'Auto-Investigate (migrated)'
  );
