UPDATE llm_prompt_versions SET temperature = 0.5 WHERE temperature IS NULL;
UPDATE llm_prompt_versions SET commit_message = 'Initial version' WHERE commit_message IS NULL;

ALTER TABLE llm_prompt_versions
  ALTER COLUMN temperature SET NOT NULL,
  ALTER COLUMN temperature SET DEFAULT 0.5;

ALTER TABLE llm_prompt_versions
  ALTER COLUMN commit_message SET NOT NULL,
  ALTER COLUMN commit_message SET DEFAULT '';
