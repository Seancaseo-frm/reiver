-- Add Stripe Price ID to tier definitions so each tier maps to a Stripe recurring price.
ALTER TABLE tier_definitions ADD COLUMN stripe_price_id TEXT;
