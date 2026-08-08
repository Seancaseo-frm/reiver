-- Drop the platform_fees table.
-- BYOK fees are now computed from ClickHouse llm_cost_daily at invoice time;
-- no application code reads or writes this table.
DROP TABLE IF EXISTS platform_fees;

-- Remove the 'platform_fee' value from credit_transaction_type enum.
-- No rows of this type exist (unreleased feature).
ALTER TYPE credit_transaction_type RENAME TO credit_transaction_type_old;

CREATE TYPE credit_transaction_type AS ENUM (
    'top_up',
    'usage_deduction',
    'refund',
    'adjustment'
);

ALTER TABLE credit_transactions
    ALTER COLUMN transaction_type TYPE credit_transaction_type
    USING transaction_type::text::credit_transaction_type;

DROP TYPE credit_transaction_type_old;
