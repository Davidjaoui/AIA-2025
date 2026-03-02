FROM apache/airflow:2.10.2

USER airflow

# Installer directement dans le virtualenv d'Airflow
RUN pip install --no-cache-dir \
    mlflow==2.21.3 \
    xgboost==2.1.4 \
    numpy==2.0.2 \
    pandas==2.3.3 \
    scipy==1.13.1 \
    tqdm \
    boto3 \
    geopy
