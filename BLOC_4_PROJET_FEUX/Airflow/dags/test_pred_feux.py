from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.hooks.base import BaseHook
import pandas as pd
import numpy as np
import mlflow
import mlflow.pyfunc
import os

# ------------------------------------------------------ Config ------------------------------------------------------
aws_conn_id = "aws_default"  # connexion Airflow pour S3/MLflow
aws_region = "eu-west-3"

bucket = "projet-final-lead"
source_key = "data/dataset_complet_meteo.csv"
out_key = "predictions/predictions_incendies.csv"

mlflow.set_tracking_uri("https://djohell-ml-flow.hf.space")
model_uri = "runs:/69a3c889954f4ce9a2139a4fb4cefc59/survival_xgb_model"

# ------------------------------------------------------ Functions ------------------------------------------------------
def load_data_from_s3():
    """Charge le CSV depuis S3 via le hook Airflow."""
    hook = S3Hook(aws_conn_id=aws_conn_id)
    obj = hook.get_key(source_key, bucket_name=bucket)
    df = pd.read_csv(obj.get()['Body'], sep=";")
    for col in df.columns:
        if "date" in col.lower():
            df[col] = pd.to_datetime(df[col], errors="coerce", dayfirst=True)
    return df

def load_model():
    """Charge le modèle MLflow depuis S3 en utilisant la connexion Airflow pour les creds."""
    # Récupère les credentials depuis Airflow
    conn = BaseHook.get_connection(aws_conn_id)
    os.environ["AWS_ACCESS_KEY_ID"] = conn.login
    os.environ["AWS_SECRET_ACCESS_KEY"] = conn.password
    os.environ["AWS_DEFAULT_REGION"] = aws_region

    return mlflow.pyfunc.load_model(model_uri)

def generate_predictions():
    df = load_data_from_s3().copy()
    model = load_model()

    df = df.rename(columns={"Feu prévu": "event", "décompte": "duration"})
    df["event"] = df["event"].astype(bool)
    df["duration"] = df["duration"].fillna(0)

    features = [
        "moyenne precipitations mois", "moyenne temperature mois",
        "moyenne evapotranspiration mois", "moyenne vitesse vent année",
        "moyenne vitesse vent mois", "moyenne temperature année",
        "RR", "UM", "ETPMON", "TN", "TX", "Nombre de feu par an",
        "Nombre de feu par mois", "jours_sans_pluie", "jours_TX_sup_30",
        "ETPGRILLE_7j", "compteur jours vers prochain feu",
        "compteur feu log", "Année", "Mois",
        "moyenne precipitations année", "moyenne evapotranspiration année",
    ]
    features = [f for f in features if f in df.columns]

    log_hr = model.predict(df[features])
    HR = np.exp(log_hr)

    def S0(t):
        return np.exp(-t / 1000)

    horizons = {7: "proba_7j", 30: "proba_30j", 60: "proba_60j", 90: "proba_90j", 180: "proba_180j"}
    for t, col in horizons.items():
        df[col] = 1 - (S0(t) ** HR)

    for col in ["latitude", "longitude", "ville"]:
        if col not in df.columns:
            df[col] = np.nan

    df_out = df[["latitude", "longitude", "ville"] + list(horizons.values())].copy()

    tmp_path = "/tmp/predictions_incendies.csv"
    df_out.to_csv(tmp_path, index=False)

    # Upload avec le hook Airflow
    hook = S3Hook(aws_conn_id=aws_conn_id)
    hook.load_file(tmp_path, key=out_key, bucket_name=bucket, replace=True)

# ------------------------------------------------------ DAG ------------------------------------------------------
default_args = {
    "owner": "admin",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="predictions_incendies_dag",
    default_args=default_args,
    description="Génère les prédictions incendies et stocke le CSV dans S3",
    schedule_interval="0 2 * * *",  # tous les jours à 2h
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    task_generate_predictions = PythonOperator(
        task_id="generate_predictions",
        python_callable=generate_predictions,
    )

    task_generate_predictions

