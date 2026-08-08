-- Credit System Hardening
--
-- 1. UNIQUE partial index on stripe_checkout_session_id to prevent double-crediting
-- 2. UNIQUE partial index on llm_request_id for credit_transactions (deduction idempotency)
-- 3. UNIQUE partial index on llm_request_id for platform_fees (BYOK fee idempotency)

-- Prevent double-crediting from duplicate Stripe webhook deliveries
DROP INDEX IF EXISTS idx_credit_transactions_stripe_session;
CREATE UNIQUE INDEX idx_credit_transactions_stripe_session
    ON credit_transactions(stripe_checkout_session_id)
    WHERE stripe_checkout_session_id IS NOT NULL;

-- Prevent duplicate deduction ledger entries from retried tokio::spawn tasks
CREATE UNIQUE INDEX idx_credit_transactions_llm_request
    ON credit_transactions(llm_request_id)
    WHERE llm_request_id IS NOT NULL;

-- Prevent duplicate BYOK fee entries
CREATE UNIQUE INDEX idx_platform_fees_llm_request
    ON platform_fees(llm_request_id)
    WHERE llm_request_id IS NOT NULL;
