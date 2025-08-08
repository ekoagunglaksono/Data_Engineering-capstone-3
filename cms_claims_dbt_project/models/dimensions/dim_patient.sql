SELECT
    beneficiary_id,
    birth_date,
    EXTRACT(YEAR FROM CURRENT_DATE()) - EXTRACT(YEAR FROM birth_date) AS age, -- Menghitung usia saat ini
    death_date,
    gender,
    race_code,
    state_code,
    county_code,
    com_alzheimers,
    com_heart_failure,
    com_kidney_disease,
    com_cancer,
    com_copd,
    com_depression,
    com_diabetes,
    com_ischemic_heart,
    com_osteoporosis,
    com_arthritis,
    com_stroke
FROM {{ ref('stg_cms_claims') }}
QUALIFY ROW_NUMBER() OVER (PARTITION BY beneficiary_id ORDER BY claim_start_date DESC) = 1