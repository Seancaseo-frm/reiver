-- ============================================================================
-- Insert Theta EdgeCloud on-demand models into model_catalog.
-- These are not available via OpenRouter, so they must be added manually.
-- Model IDs use the theta/{service_alias} convention matching the gateway
-- provider adapter (flow/src/gateway/providers/theta.rs).
-- ============================================================================

INSERT INTO model_catalog (id, name, provider_slug, model_slug, enabled, pricing, description)
VALUES
  ('theta/llama_3_8b',    'Llama 3 8B',    'theta', 'llama_3_8b',    TRUE, '{}', 'Meta Llama 3 8B via Theta EdgeCloud on-demand inference'),
  ('theta/llama_3_1_70b', 'Llama 3.1 70B', 'theta', 'llama_3_1_70b', TRUE, '{}', 'Meta Llama 3.1 70B via Theta EdgeCloud on-demand inference'),
  ('theta/qwen3',         'Qwen3',         'theta', 'qwen3',         TRUE, '{}', 'Alibaba Qwen3 via Theta EdgeCloud on-demand inference'),
  ('theta/gpt_oss_120b',  'GPT OSS 120B',  'theta', 'gpt_oss_120b',  TRUE, '{}', 'OpenAI GPT OSS 120B via Theta EdgeCloud on-demand inference'),
  ('theta/minimax_m2_5',  'MiniMax M2.5',  'theta', 'minimax_m2_5',  TRUE, '{}', 'MiniMax M2.5 via Theta EdgeCloud on-demand inference')
ON CONFLICT (id) DO NOTHING;
