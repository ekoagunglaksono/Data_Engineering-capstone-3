SELECT
    -- Kunci & ID
    desynpuf_id,
    clm_id,
    
    -- Informasi Tanggal
    CAST(clm_from_dt AS DATE) AS claim_date,
    CAST(bene_birth_dt AS DATE) AS patient_birth_date,
    
    -- Informasi Pasien
    CAST(bene_sex_ident_cd AS INT) AS patient_gender_code,
    CAST(bene_race_cd AS INT) AS patient_race_cd,
    CAST(sp_state_code AS INT) AS patient_state_code, 
    CAST(bene_county_cd AS INT) AS patient_county_code,
    
    -- Informasi Klaim & Biaya
    -- Karena tidak ada kolom 'total_claim_cost', saya hitung dari beberapa kolom pembayaran
    COALESCE(medreimb_ip, 0) + COALESCE(medreimb_op, 0) + COALESCE(medreimb_car, 0) AS total_claim_cost,
    
    -- Tambahan kolom lain yang relevan 
    CAST(hcpcs_cd_1 AS STRING) AS hcpcs_code,
    CAST(icd9_dgns_cd_1 AS STRING) AS diagnosis_code,
    CAST(clm_thru_dt AS DATE) AS claim_end_date
    
FROM {{ source('cms_raw_data', 'cms_claims_raw') }}