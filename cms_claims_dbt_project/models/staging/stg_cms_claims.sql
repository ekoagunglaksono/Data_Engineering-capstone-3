SELECT
    desynpuf_id AS beneficiary_id,
    clm_id AS claim_id,
    CAST(clm_from_dt AS DATE) AS claim_start_date,
    CAST(clm_thru_dt AS DATE) AS claim_end_date,
    -- Menentukan jenis klaim berdasarkan kolom non-null
    CASE
        WHEN medreimb_ip IS NOT NULL THEN 'Inpatient'
        WHEN medreimb_op IS NOT NULL THEN 'Outpatient'
        WHEN prf_physn_npi_1 IS NOT NULL THEN 'Carrier'
        ELSE 'Unknown'
    END AS claim_type,
    -- Menghitung total_claim_cost berdasarkan jenis klaim
    COALESCE(medreimb_ip, 0) + COALESCE(benres_ip, 0) + COALESCE(pppymt_ip, 0) +
    COALESCE(medreimb_op, 0) + COALESCE(benres_op, 0) + COALESCE(pppymt_op, 0) +
    COALESCE(line_nch_pmt_amt_1, 0) + COALESCE(line_bene_ptb_ddctbl_amt_1, 0) + COALESCE(line_coinsrnc_amt_1, 0)
    AS total_claim_cost,
    
    -- Mengambil kolom-kolom demografi dan lainnya
    SAFE_CAST(CAST(bene_birth_dt AS STRING) AS DATE) AS birth_date,
    SAFE_CAST(CAST(bene_death_dt AS STRING) AS DATE) AS death_date,
    CASE bene_sex_ident_cd
        WHEN 1 THEN 'M'
        WHEN 2 THEN 'F'
        ELSE 'U'
    END AS gender,
    bene_race_cd AS race_code,
    sp_state_code AS state_code,
    bene_county_cd AS county_code,
    sp_alzhdmta AS com_alzheimers,
    sp_chf AS com_heart_failure,
    sp_chrnkidn AS com_kidney_disease,
    sp_cncr AS com_cancer,
    sp_copd AS com_copd,
    sp_depressn AS com_depression,
    sp_diabetes AS com_diabetes,
    sp_ischmcht AS com_ischemic_heart,
    sp_osteoprs AS com_osteoporosis,
    sp_ra_oa AS com_arthritis,
    sp_strketia AS com_stroke,
    prf_physn_npi_1 AS provider_id,
    icd9_dgns_cd_1 AS diagnosis_code,
    hcpcs_cd_1 AS hcpcs_code
FROM {{ source('cms_raw_data', 'cms_claims_raw') }}
WHERE
    COALESCE(medreimb_ip, 0) + COALESCE(benres_ip, 0) + COALESCE(pppymt_ip, 0) >= 0 AND
    COALESCE(medreimb_op, 0) + COALESCE(benres_op, 0) + COALESCE(pppymt_op, 0) >= 0 AND
    COALESCE(line_nch_pmt_amt_1, 0) + COALESCE(line_bene_ptb_ddctbl_amt_1, 0) + COALESCE(line_coinsrnc_amt_1, 0) >= 0