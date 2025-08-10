import os
import requests
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago

# Ambil webhook dari Airflow Variables
DISCORD_WEBHOOK_URL = Variable.get("DISCORD_WEBHOOK_URL")

# Fungsi alert ke Discord jika task gagal
def discord_alert(context):
    task_id = context.get('task_instance').task_id
    dag_id = context.get('dag').dag_id
    log_url = context.get('task_instance').log_url
    
    message = f":red_circle: Task **{task_id}** di DAG **{dag_id}** gagal!\nLihat log: {log_url}"
    try:
        requests.post(DISCORD_WEBHOOK_URL, json={"content": message})
    except Exception as e:
        print(f"Gagal mengirim alert Discord: {e}")

# Default args dengan on_failure_callback
default_args = {
    "on_failure_callback": discord_alert
}

# Konfigurasi
AIRFLOW_HOME = "/opt/airflow"

# Definisikan DAG
with DAG(
    dag_id="elt_cms_claims_dag",
    start_date=days_ago(1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    tags=["dbt", "bigquery"],
) as dag:

    # Task 1: Menjalankan skrip download data dari Kaggle
    download_data_task = BashOperator(
        task_id="download_data_from_kaggle",
        bash_command=f"python {AIRFLOW_HOME}/scripts/download_kaggle_data.py",
    )

    # Task 2: Menjalankan skrip ingest Python
    ingest_data_task = BashOperator(
        task_id="ingest_data_to_bigquery",
        bash_command=f"python {AIRFLOW_HOME}/scripts/ingest_to_bigquery.py",
    )

    # Task 3: Menjalankan dbt run untuk transformasi data
    run_dbt_models_task = BashOperator(
        task_id="run_dbt_models",
        bash_command=f"cd {AIRFLOW_HOME}/cms_claims_dbt_project && dbt run --profiles-dir {AIRFLOW_HOME}/.dbt",
    )

    # Task 4: Menjalankan dbt test untuk data quality checks
    run_dbt_tests_task = BashOperator(
        task_id="run_dbt_tests",
        bash_command=f"cd {AIRFLOW_HOME}/cms_claims_dbt_project && dbt test --profiles-dir {AIRFLOW_HOME}/.dbt",
    )

    # Task 5: Mengirim notifikasi kegagalan ke Discord
    test_failure_task = BashOperator(
        task_id="test_failure_task",
        bash_command="exit 1",  
    )

    # Definisi urutan task
    download_data_task >> ingest_data_task >> run_dbt_models_task >> run_dbt_tests_task >> test_failure_task
