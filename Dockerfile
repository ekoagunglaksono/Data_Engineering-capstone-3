# Gunakan image resmi Airflow versi stabil + Python 3.10
FROM apache/airflow:2.7.1-python3.10

# Salin requirements.txt ke container
COPY requirements.txt /requirements.txt

# Install semua dependency tambahan
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r /requirements.txt