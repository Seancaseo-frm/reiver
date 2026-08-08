-- Flow Credit System
--
-- Implements a prepaid credit wallet for Flow LLM gateway usage.
-- Platform-key requests deduct from the wallet; BYOK requests incur a 3% platform fee.

-- ============================================================================
-- CREDIT WALLETS: One row per organization, tracks current balance
-- ============================================================================

CREATE TABLE credit_wallets (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id UUID NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
    balance_usd NUMERIC(18, 8) NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT credit_wallets_organization_unique UNIQUE (organization_id),
    CONSTRAINT credit_wallets_balance_non_negative CHECK (balance_usd >= 0)
);

CREATE INDEX idx_credit_wallets_org ON credit_wallets(organization_id);

-- ============================================================================
-- CREDIT TRANSACTIONS: Append-only ledger of all balance changes
-- ============================================================================

CREATE TYPE credit_transaction_type AS ENUM (
    'top_up',
    'usage_deduction',
    'platform_fee',
    'refund',
    'adjustment'
);

CREATE TABLE credit_transactions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id UUID NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
    transaction_type credit_transaction_type NOT NULL,
    amount_usd NUMERIC(18, 8) NOT NULL,
    balance_after_usd NUMERIC(18, 8) NOT NULL,
    description TEXT,

    -- Stripe payment details (for top_up transactions)
    stripe_checkout_session_id VARCHAR,
    paid_amount NUMERIC(18, 8),
    paid_currency CHAR(3),
    exchange_rate NUMERIC(18, 8),

    -- LLM request details (for usage_deduction transactions)
    llm_request_id VARCHAR,
    project_id UUID,
    provider VARCHAR,
    model VARCHAR,
    input_tokens INT,
    output_tokens INT,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_credit_transactions_org_created ON credit_transactions(organization_id, created_at DESC);
CREATE INDEX idx_credit_transactions_stripe_session ON credit_transactions(stripe_checkout_session_id)
    WHERE stripe_checkout_session_id IS NOT NULL;

-- ============================================================================
-- PLATFORM FEES: Tracks 3% fee on BYOK requests for periodic invoicing
-- ============================================================================

CREATE TABLE platform_fees (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id UUID NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
    project_id UUID NOT NULL,
    provider VARCHAR NOT NULL,
    model VARCHAR NOT NULL,
    input_tokens INT NOT NULL DEFAULT 0,
    output_tokens INT NOT NULL DEFAULT 0,
    cost_usd NUMERIC(18, 8) NOT NULL,
    fee_usd NUMERIC(18, 8) NOT NULL,
    llm_request_id VARCHAR,
    invoiced BOOLEAN NOT NULL DEFAULT FALSE,
    invoice_id UUID,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_platform_fees_org_invoiced ON platform_fees(organization_id, invoiced, created_at);
