SELECT
    claim_id,
    beneficiary_id,
    claim_type,
    claim_start_date AS date_key,
    total_claim_cost
FROM {{ ref('stg_cms_claims') }}
WHERE claim_id IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_id ORDER BY claim_start_date DESC) = 1