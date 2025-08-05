import os
import subprocess
import shutil

DATASET_NAME = "drscarlat/medicalclaimssynthetic1m"
DOWNLOAD_PATH = "data/raw_source"

def download_and_extract_kaggle_dataset(dataset, path):
    """
    Mengunduh dataset dari Kaggle CLI dan mengekstraknya.
    """
    print(f"--- Memulai pengunduhan dataset: {dataset} ---")
    try:
        os.makedirs(path, exist_ok=True)

        subprocess.run(
            ["kaggle", "datasets", "download", "-d", dataset, "-p", path, "--unzip"],
            check=True 
        )
        print("Pengunduhan dan ekstraksi berhasil.")

    except FileNotFoundError:
        print("Error: Perintah 'kaggle' tidak ditemukan.")
        print("Pastikan Anda sudah menginstal 'kaggle' CLI dengan 'pip install kaggle'.")
    except subprocess.CalledProcessError as e:
        print(f"Error saat menjalankan perintah kaggle: {e}")
        print("Pastikan API key Anda sudah benar dan memiliki akses ke dataset.")

if __name__ == "__main__":
    download_and_extract_kaggle_dataset(DATASET_NAME, DOWNLOAD_PATH)
    print("\n--- Proses pengunduhan ke lokal selesai ---")