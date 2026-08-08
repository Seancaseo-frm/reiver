ALTER TABLE agent_messages
    ADD CONSTRAINT chk_agent_messages_role
    CHECK (role IN ('system', 'user', 'assistant', 'tool'));
