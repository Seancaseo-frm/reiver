-- Combine all non-credit charges into a single row per org per billing period.
-- Remove duplicate rows for the same org+period before adding the stricter constraint.
-- Only unpaid charges (pending, payment_failed) are deleted; paid/approved charges are kept.

-- Delete unpaid duplicate charges, keeping only one per (org, period).
-- For each (org, period) group with multiple rows, keep the one with the highest amount.
DELETE FROM pending_charges
WHERE id IN (
    SELECT id FROM (
        SELECT id,
               ROW_NUMBER() OVER (
                   PARTITION BY organization_id, billing_period_start
                   ORDER BY amount_usd DESC
               ) AS rn
        FROM pending_charges
        WHERE status IN ('pending', 'payment_failed')
    ) sub
    WHERE rn > 1
);

-- Also delete any remaining unpaid old-style charges that would conflict
-- with a future combined charge for the same period.
-- (If there's a paid charge for one type and an unpaid for another, drop the unpaid one.)
DELETE FROM pending_charges
WHERE status IN ('pending', 'payment_failed')
  AND (organization_id, billing_period_start) IN (
      SELECT organization_id, billing_period_start
      FROM pending_charges
      GROUP BY organization_id, billing_period_start
      HAVING COUNT(*) > 1
  );

ALTER TABLE pending_charges
    DROP CONSTRAINT IF EXISTS pending_charges_organization_id_charge_type_billing_period__key;

ALTER TABLE pending_charges
    ADD CONSTRAINT pending_charges_organization_id_billing_period_start_key
    UNIQUE (organization_id, billing_period_start);
