FROM python:3.9-slim

RUN pip install mlflow

WORKDIR /mlflow

# Expose le port 5000 pour MLflow
EXPOSE 5000

# Lancement du serveur MLflow avec une base sqlite locale
CMD mlflow server \
    --backend-store-uri sqlite:///mlflow.db \
    --default-artifact-root ./mlruns \
    --host 0.0.0.0 \
    --port 5000
