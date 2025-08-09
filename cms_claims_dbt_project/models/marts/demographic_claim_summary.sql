SELECT
    p.gender AS patient_gender,
    p.state_code AS patient_state,
    CASE
        WHEN p.age < 18 THEN '0-18'
        WHEN p.age BETWEEN 18 AND 35 THEN '18-35'
        WHEN p.age BETWEEN 36 AND 55 THEN '36-55'
        ELSE '55+'
    END AS patient_age_group,
    SUM(f.total_claim_cost) AS total_claim_cost,
    COUNT(f.claim_id) AS total_claim_count
FROM {{ ref('fct_claims') }} AS f
JOIN {{ ref('dim_patient') }} AS p
  ON f.beneficiary_id = p.beneficiary_id
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3