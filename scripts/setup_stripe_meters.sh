#!/usr/bin/env bash
# =============================================================================
# Stripe Metering Setup Script
# =============================================================================
#
# Creates the 3 Stripe Billing Meters and graduated tiered prices needed for
# the Reiver metering system.
#
# Prerequisites:
#   - STRIPE_API_KEY env var set (use test mode key for staging)
#   - `curl` and `jq` available
#
# This script is idempotent — Stripe will error if meters already exist
# with the same event_name; you can safely ignore those errors on re-run.
#
# After running, copy the printed price IDs into your environment/config:
#   STRIPE_CREDITS_PRICE_STARTER=price_xxx
#   STRIPE_CREDITS_PRICE_SCALE=price_xxx
#   STRIPE_SCANS_PRICE_STARTER=price_xxx
#   STRIPE_SCANS_PRICE_SCALE=price_xxx
#   STRIPE_OBS_GB_PRICE_STARTER=price_xxx
#   STRIPE_OBS_GB_PRICE_SCALE=price_xxx
# =============================================================================

set -euo pipefail

: "${STRIPE_API_KEY:?Set STRIPE_API_KEY}"

API="https://api.stripe.com/v1"

stripe_post() {
    curl -s -X POST "$API/$1" \
        -u "$STRIPE_API_KEY:" \
        -H "Content-Type: application/x-www-form-urlencoded" \
        "${@:2}"
}

echo "=== Creating Stripe Billing Meters ==="

echo "1. Creating moodeng_credits meter..."
stripe_post "billing/meters" \
    -d "display_name=MooDeng Credits" \
    -d "event_name=moodeng_credits" \
    -d "default_aggregation[formula]=sum" \
    -d "customer_mapping[type]=by_id" \
    -d "customer_mapping[event_payload_key]=stripe_customer_id" \
    -d "value_settings[event_payload_key]=value" \
| jq '{id, display_name, event_name, status}'

echo ""
echo "2. Creating session_scans meter..."
stripe_post "billing/meters" \
    -d "display_name=Session Scans" \
    -d "event_name=session_scans" \
    -d "default_aggregation[formula]=sum" \
    -d "customer_mapping[type]=by_id" \
    -d "customer_mapping[event_payload_key]=stripe_customer_id" \
    -d "value_settings[event_payload_key]=value" \
| jq '{id, display_name, event_name, status}'

echo ""
echo "3. Creating observability_gb meter..."
stripe_post "billing/meters" \
    -d "display_name=Observability GB" \
    -d "event_name=observability_gb" \
    -d "default_aggregation[formula]=sum" \
    -d "customer_mapping[type]=by_id" \
    -d "customer_mapping[event_payload_key]=stripe_customer_id" \
    -d "value_settings[event_payload_key]=value" \
| jq '{id, display_name, event_name, status}'

echo ""
echo "=== Creating Products ==="

PRODUCT_CREDITS=$(stripe_post "products" \
    -d "name=MooDeng Credits" \
    -d "metadata[meter]=moodeng_credits" \
| jq -r '.id')
echo "Credits product: $PRODUCT_CREDITS"

PRODUCT_SCANS=$(stripe_post "products" \
    -d "name=Session Scans" \
    -d "metadata[meter]=session_scans" \
| jq -r '.id')
echo "Scans product: $PRODUCT_SCANS"

PRODUCT_OBS=$(stripe_post "products" \
    -d "name=Observability GB" \
    -d "metadata[meter]=observability_gb" \
| jq -r '.id')
echo "Observability product: $PRODUCT_OBS"

echo ""
echo "=== Creating Graduated Prices (Starter Tier) ==="

# Starter credits: 0-10000 at $0, then $0.20 each
echo "Starter credits price..."
PRICE_CREDITS_STARTER=$(stripe_post "prices" \
    -d "product=$PRODUCT_CREDITS" \
    -d "currency=usd" \
    -d "recurring[interval]=month" \
    -d "recurring[usage_type]=metered" \
    -d "billing_scheme=tiered" \
    -d "tiers_mode=graduated" \
    -d "tiers[0][up_to]=10000" \
    -d "tiers[0][unit_amount]=0" \
    -d "tiers[1][up_to]=inf" \
    -d "tiers[1][unit_amount]=20" \
    -d "metadata[tier]=starter" \
| jq -r '.id')
echo "  $PRICE_CREDITS_STARTER"

# Starter scans: 0-5000 at $0, then $0.003 each
echo "Starter scans price..."
PRICE_SCANS_STARTER=$(stripe_post "prices" \
    -d "product=$PRODUCT_SCANS" \
    -d "currency=usd" \
    -d "recurring[interval]=month" \
    -d "recurring[usage_type]=metered" \
    -d "billing_scheme=tiered" \
    -d "tiers_mode=graduated" \
    -d "tiers[0][up_to]=5000" \
    -d "tiers[0][unit_amount]=0" \
    -d "tiers[1][up_to]=inf" \
    -d "tiers[1][unit_amount_decimal]=0.3" \
    -d "metadata[tier]=starter" \
| jq -r '.id')
echo "  $PRICE_SCANS_STARTER"

# Starter observability: 0-200 GB at $0, then $0.25/GB ($250/TB)
echo "Starter observability price..."
PRICE_OBS_STARTER=$(stripe_post "prices" \
    -d "product=$PRODUCT_OBS" \
    -d "currency=usd" \
    -d "recurring[interval]=month" \
    -d "recurring[usage_type]=metered" \
    -d "billing_scheme=tiered" \
    -d "tiers_mode=graduated" \
    -d "tiers[0][up_to]=200" \
    -d "tiers[0][unit_amount]=0" \
    -d "tiers[1][up_to]=inf" \
    -d "tiers[1][unit_amount]=25" \
    -d "metadata[tier]=starter" \
| jq -r '.id')
echo "  $PRICE_OBS_STARTER"

echo ""
echo "=== Creating Graduated Prices (Scale Tier) ==="

# Scale credits: 0-100000 at $0, then $0.20 each
echo "Scale credits price..."
PRICE_CREDITS_SCALE=$(stripe_post "prices" \
    -d "product=$PRODUCT_CREDITS" \
    -d "currency=usd" \
    -d "recurring[interval]=month" \
    -d "recurring[usage_type]=metered" \
    -d "billing_scheme=tiered" \
    -d "tiers_mode=graduated" \
    -d "tiers[0][up_to]=100000" \
    -d "tiers[0][unit_amount]=0" \
    -d "tiers[1][up_to]=inf" \
    -d "tiers[1][unit_amount]=20" \
    -d "metadata[tier]=scale" \
| jq -r '.id')
echo "  $PRICE_CREDITS_SCALE"

# Scale scans: 0-30000 at $0, then $0.003 each
echo "Scale scans price..."
PRICE_SCANS_SCALE=$(stripe_post "prices" \
    -d "product=$PRODUCT_SCANS" \
    -d "currency=usd" \
    -d "recurring[interval]=month" \
    -d "recurring[usage_type]=metered" \
    -d "billing_scheme=tiered" \
    -d "tiers_mode=graduated" \
    -d "tiers[0][up_to]=30000" \
    -d "tiers[0][unit_amount]=0" \
    -d "tiers[1][up_to]=inf" \
    -d "tiers[1][unit_amount_decimal]=0.3" \
    -d "metadata[tier]=scale" \
| jq -r '.id')
echo "  $PRICE_SCANS_SCALE"

# Scale observability: 0-1000 GB at $0, then $0.25/GB
echo "Scale observability price..."
PRICE_OBS_SCALE=$(stripe_post "prices" \
    -d "product=$PRODUCT_OBS" \
    -d "currency=usd" \
    -d "recurring[interval]=month" \
    -d "recurring[usage_type]=metered" \
    -d "billing_scheme=tiered" \
    -d "tiers_mode=graduated" \
    -d "tiers[0][up_to]=1000" \
    -d "tiers[0][unit_amount]=0" \
    -d "tiers[1][up_to]=inf" \
    -d "tiers[1][unit_amount]=25" \
    -d "metadata[tier]=scale" \
| jq -r '.id')
echo "  $PRICE_OBS_SCALE"

echo ""
echo "=== Summary ==="
echo ""
echo "Add these to your environment:"
echo ""
echo "STRIPE_CREDITS_PRICE_STARTER=$PRICE_CREDITS_STARTER"
echo "STRIPE_CREDITS_PRICE_SCALE=$PRICE_CREDITS_SCALE"
echo "STRIPE_SCANS_PRICE_STARTER=$PRICE_SCANS_STARTER"
echo "STRIPE_SCANS_PRICE_SCALE=$PRICE_SCANS_SCALE"
echo "STRIPE_OBS_GB_PRICE_STARTER=$PRICE_OBS_STARTER"
echo "STRIPE_OBS_GB_PRICE_SCALE=$PRICE_OBS_SCALE"
echo ""
echo "Subscription structure: one subscription per customer with:"
echo "  - Base plan price item (flat monthly)"
echo "  - Credits metered price item"
echo "  - Scans metered price item"
echo "  - Observability GB metered price item"
echo ""
echo "Free tier: no subscription. Hard caps enforced in application code."
