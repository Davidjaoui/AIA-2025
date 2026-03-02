import os
import warnings
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.impute import SimpleImputer
from sklearn.model_selection import train_test_split
from sklearn.exceptions import UndefinedMetricWarning
from xgboost import XGBRegressor
from sksurv.util import Surv
from sksurv.metrics import concordance_index_censored
import boto3
import mlflow
import mlflow.sklearn

warnings.filterwarnings("ignore", category=UndefinedMetricWarning)

# -------------------- Configuration AWS S3 --------------------
aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
aws_region = "eu-west-3"

s3 = boto3.client(
    's3',
    region_name=aws_region,
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key
)

# -------------------- Charger les données depuis S3 --------------------
def load_model_data() -> pd.DataFrame:
    bucket = "projet-final-lead"
    key = "data/dataset_complet_meteo.csv"
    obj = s3.get_object(Bucket=bucket, Key=key)
    df = pd.read_csv(obj['Body'], sep=";")
    for col in df.columns:
        if "date" in col.lower():
            df[col] = pd.to_datetime(df[col], errors="coerce", dayfirst=True)
    return df

# -------------------- MLflow --------------------
mlflow_tracking_uri = os.environ.get("MLFLOW_TRACKING_URI")
mlflow.set_tracking_uri(mlflow_tracking_uri)
mlflow.set_experiment("survival_xgb_training")

# -------------------- Fonction d'entraînement --------------------
def train_model():
    df = load_model_data()
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

    y_struct = Surv.from_dataframe("event", "duration", df)
    X_train, X_test, y_train, y_test = train_test_split(df[features], y_struct, test_size=0.3, random_state=42)
    ev_train, du_train = y_train["event"], y_train["duration"]
    ev_test, du_test = y_test["event"], y_test["duration"]

    model = Pipeline([
        ("imputer", SimpleImputer(strategy="median")),
        ("scaler", StandardScaler()),
        ("xgb", XGBRegressor(
            objective="survival:cox",
            n_estimators=100,
            learning_rate=0.05,
            max_depth=3,
            tree_method="hist",
            random_state=42,
        )),
    ])

    with mlflow.start_run(run_name="XGBSurv_Train"):
        # Entraînement
        model.fit(X_train, du_train, xgb__sample_weight=ev_train)

        # Log hyperparams
        mlflow.log_params({"n_estimators":100,"learning_rate":0.05,"max_depth":3,"tree_method":"hist"})

        # Log modèle sklearn
        mlflow.sklearn.log_model(model, artifact_path="survival_xgb_model")

        # Log métriques
        log_hr_test = model.predict(X_test)
        c_index = concordance_index_censored(ev_test, du_test, log_hr_test)[0]
        print(f"C-index (test) : {c_index:.3f}")
        mlflow.log_metric("c_index_test", c_index)

        # Log figure comme artifact
        fig, ax = plt.subplots(figsize=(6,4))
        sns.histplot(log_hr_test, bins=30, ax=ax)
        ax.set_title("Distribution log hazard (test)")
        fig_path = "log_hazard_test.png"
        fig.savefig(fig_path)
        plt.close(fig)
        mlflow.log_artifact(fig_path)

        # Log CSV prédictions
        df_test_pred = pd.DataFrame({"duration": du_test, "event": ev_test, "log_hazard_pred": log_hr_test})
        csv_path = "predictions_test.csv"
        df_test_pred.to_csv(csv_path, index=False)
        mlflow.log_artifact(csv_path)

    print("✅ Entraînement terminé et artifacts/logs envoyés à MLflow !")

# -------------------- Lancer l'entraînement --------------------
if __name__ == "__main__":
    train_model()
