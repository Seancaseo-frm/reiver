-- Billing overhaul: size-based Watch pricing + pending charges for admin approval.

-- 1. New pricing columns for size-based billing.
--    Traces/logs: $0.20 per GB.  Metrics: $0.05 per million.
--    Old per-million columns are kept for historical audit.
ALTER TABLE billing_pricing
    ADD COLUMN IF NOT EXISTS traces_logs_per_gb_usd NUMERIC(10,4) NOT NULL DEFAULT 0.2000,
    ADD COLUMN IF NOT EXISTS metrics_per_million_usd NUMERIC(10,4) NOT NULL DEFAULT 0.0500;

-- 2. Add ingested-byte columns to daily snapshots (alongside existing event counts).
ALTER TABLE usage_daily_snapshots
    ADD COLUMN IF NOT EXISTS spans_ingested_bytes BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS logs_ingested_bytes BIGINT NOT NULL DEFAULT 0;

-- 3. Pending charges table for monthly billing with admin approval.
CREATE TABLE IF NOT EXISTS pending_charges (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id UUID NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
    charge_type TEXT NOT NULL,             -- 'watch_usage' | 'flow_byok_fees'
    billing_period_start DATE NOT NULL,
    billing_period_end DATE NOT NULL,
    amount_usd NUMERIC(18,8) NOT NULL,
    description TEXT,
    line_items JSONB,
    status TEXT NOT NULL DEFAULT 'pending', -- pending | approved | rejected | paid | payment_failed
    reviewed_by UUID REFERENCES users(id),
    reviewed_at TIMESTAMPTZ,
    stripe_payment_intent_id TEXT,
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(organization_id, charge_type, billing_period_start)
);

CREATE INDEX IF NOT EXISTS idx_pending_charges_org ON pending_charges(organization_id);
CREATE INDEX IF NOT EXISTS idx_pending_charges_status ON pending_charges(status);

CREATE TRIGGER trigger_pending_charges_updated_at
    BEFORE UPDATE ON pending_charges
    FOR EACH ROW
    EXECUTE FUNCTION update_billing_updated_at();
