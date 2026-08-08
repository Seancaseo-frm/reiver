-- Align provider slugs with OpenRouter naming convention

UPDATE llm_provider_integrations SET provider = 'x-ai' WHERE provider = 'xai';
UPDATE llm_provider_integrations SET provider = 'qwen' WHERE provider = 'alibaba';
UPDATE llm_provider_integrations SET provider = 'mistralai' WHERE provider = 'mistral';
UPDATE llm_provider_integrations SET provider = 'theta-dedicated' WHERE provider = 'theta_dedicated';
UPDATE llm_provider_integrations SET provider = 'azure-openai' WHERE provider = 'azure_openai';
UPDATE llm_provider_integrations SET provider = 'vertex-ai' WHERE provider = 'vertex_ai';

UPDATE project_settings SET key = REPLACE(key, 'gateway_xai_', 'gateway_x-ai_') WHERE key LIKE 'gateway_xai_%';
UPDATE project_settings SET key = REPLACE(key, 'gateway_alibaba_', 'gateway_qwen_') WHERE key LIKE 'gateway_alibaba_%';
UPDATE project_settings SET key = REPLACE(key, 'gateway_mistral_', 'gateway_mistralai_') WHERE key LIKE 'gateway_mistral_%';
UPDATE project_settings SET key = REPLACE(key, 'gateway_theta_dedicated_', 'gateway_theta-dedicated_') WHERE key LIKE 'gateway_theta_dedicated_%';
UPDATE project_settings SET key = REPLACE(key, 'gateway_azure_openai_', 'gateway_azure-openai_') WHERE key LIKE 'gateway_azure_openai_%';
UPDATE project_settings SET key = REPLACE(key, 'gateway_vertex_ai_', 'gateway_vertex-ai_') WHERE key LIKE 'gateway_vertex_ai_%';

-- Update old provider slugs inside JSON values for provider_preferences and fallback_order
UPDATE project_settings
  SET value = REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
    value,
    '"xai"', '"x-ai"'),
    '"alibaba"', '"qwen"'),
    '"mistral"', '"mistralai"'),
    '"theta_dedicated"', '"theta-dedicated"'),
    '"azure_openai"', '"azure-openai"'),
    '"vertex_ai"', '"vertex-ai"')
  WHERE key IN ('gateway_provider_preferences', 'gateway_fallback_order')
    AND (value LIKE '%"xai"%'
      OR value LIKE '%"alibaba"%'
      OR value LIKE '%"mistral"%'
      OR value LIKE '%"theta_dedicated"%'
      OR value LIKE '%"azure_openai"%'
      OR value LIKE '%"vertex_ai"%');
