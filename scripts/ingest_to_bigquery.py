import os
import pandas as pd
from google.cloud import bigquery

# --- Konfigurasi GCP ---
YOUR_GCP_PROJECT_ID = "purwadika" 
BIGQUERY_RAW_DATASET = "jcdeol005_capstone3_eko_raw"
RAW_TABLE_NAME = "cms_claims_raw"
CREDENTIALS_PATH = "purwadika-502e43f1636d.json"

# --- Konfigurasi Sumber Data Lokal ---
LOCAL_DATA_FILE = "data/raw_source/MedicalClaimsSynthetic1M.csv"

def ingest_local_csv_to_bigquery(file_path, table_name):
    """
    Membaca data dari file CSV lokal dan mengunggahnya ke BigQuery raw layer.
    """
    print(f"--- Memulai ingest untuk tabel: {table_name} dari file: {file_path} ---")
    if not os.path.exists(file_path):
        print(f"Error: File tidak ditemukan di {file_path}.")
        return

    try:
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = CREDENTIALS_PATH
        bq_client = bigquery.Client(project=YOUR_GCP_PROJECT_ID)
        bq_table_id = f"{YOUR_GCP_PROJECT_ID}.{BIGQUERY_RAW_DATASET}.{table_name}"

        print("Membaca file CSV ke dalam DataFrame. Ini mungkin memakan waktu...")
        df = pd.read_csv(file_path)

        df.columns = [
            col.lower()
               .replace(' ', '_')
               .replace('-', '_')
               .replace('.', '_')
               .replace('(', '')
               .replace(')', '')
               .strip()
            for col in df.columns
        ]

        date_columns = ['bene_birth_dt', 'claim_start_dt', 'claim_end_dt', 'clm_admsn_dt', 'clm_from_dt', 'clm_thru_dt', 'carr_clm_entry_dt', 'clm_thru_dt', 'fiss_clm_aprvl_dt', 'nch_clm_type_cd', 'clm_admsn_dt']
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce', format='%Y%m%d')

        print(f"Berhasil membaca {len(df)} baris dari {file_path}.")


        job_config = bigquery.LoadJobConfig(
            autodetect=True,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )

    
        print("Memulai proses upload ke BigQuery. Ini bisa memakan waktu...")
        load_job = bq_client.load_table_from_dataframe(
            df, bq_table_id, job_config=job_config
        )

        load_job.result()

        print(f"Data {table_name} berhasil diupload ke BigQuery table: {bq_table_id}")

    except Exception as e:
        print(f"Error saat mengunggah data {table_name}: {e}")
        raise

if __name__ == "__main__":
    ingest_local_csv_to_bigquery(LOCAL_DATA_FILE, RAW_TABLE_NAME)
    print("\n--- Proses ingest data ke BigQuery raw layer selesai ---")