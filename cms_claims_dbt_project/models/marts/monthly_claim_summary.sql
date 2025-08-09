SELECT
    d.year,
    d.month,
    d.month_name,
    p.gender AS patient_gender,
    p.state_code AS patient_state,
    SUM(f.total_claim_cost) AS total_monthly_claim_cost,
    COUNT(f.claim_id) AS total_claim_count
FROM {{ ref('fct_claims') }} AS f
JOIN {{ ref('dim_date') }} AS d
  ON f.date_key = d.date_key
JOIN {{ ref('dim_patient') }} AS p
  ON f.beneficiary_id = p.beneficiary_id
GROUP BY 1, 2, 3, 4, 5
ORDER BY 1, 2, 4, 5