import sys
import pandas as pd
import numpy as np
import joblib
import __main__
from fastapi import FastAPI
from fastapi.responses import RedirectResponse
from pydantic import BaseModel, Field
from sklearn.base import BaseEstimator, TransformerMixin

# --- 1. CLASSE PERSONNALISÉE ---
class DFToHashedTokens(BaseEstimator, TransformerMixin):
    def __init__(self, columns=None):
        self.columns = columns
    
    def fit(self, X, y=None):
        return self
    
    def transform(self, X):
        if not isinstance(X, pd.DataFrame):
            X = pd.DataFrame(X)
        X = X.copy()
        cols = getattr(self, 'columns', None)
        if cols is not None:
            return X[cols].astype(str).values.tolist()
        return X.astype(str).values.tolist()

__main__.DFToHashedTokens = DFToHashedTokens

# --- 2. CHARGEMENT DU MODÈLE ---
try:
    model = joblib.load('fraud_model_hashing.pkl')
except Exception as e:
    model = None

# --- 3. CONFIGURATION API ---
app = FastAPI(title="Fraud Detection API")

class Transaction(BaseModel):
    amt: float = Field(..., example=85.20)
    trans_date_trans_time: str = Field(..., example="2024-02-18 14:30:00")
    dob: str = Field(..., example="1985-05-20")
    lat: float = Field(..., example=48.8566)
    long: float = Field(..., example=2.3522)
    merch_lat: float = Field(..., example=48.8584)
    merch_long: float = Field(..., example=2.2945)
    city_pop: float = Field(..., example=2000000)
    category: str = Field(..., example="shopping_net")
    gender: str = Field(..., example="F")
    state: str = Field(..., example="NY")
    merchant: str = Field(..., example="Amazon")
    job: str = Field(..., example="Data Scientist")
    cc_num: int = Field(..., example=1234567890123456)

@app.get("/", include_in_schema=False)
def root():
    return RedirectResponse(url="/docs")

# --- 4. LOGIQUE DE PRÉPARATION ---
def prepare_input(data: dict):
    df = pd.DataFrame([data])
    dt = pd.to_datetime(df["trans_date_trans_time"], errors='coerce')
    dob = pd.to_datetime(df["dob"], errors='coerce')
    
    if dt.isna().any() or dob.isna().any():
        raise ValueError("Format de date invalide.")

    df["hour"] = dt.dt.hour
    df["day_of_week"] = dt.dt.dayofweek
    df["day"] = dt.dt.day
    df["month"] = dt.dt.month
    df["age"] = ((dt - dob).dt.days / 365.25).astype("float32")
    
    lat1, lon1 = np.radians(df["lat"]), np.radians(df["long"])
    lat2, lon2 = np.radians(df["merch_lat"]), np.radians(df["merch_long"])
    d = np.sin((lat2-lat1)/2)**2 + np.cos(lat1)*np.cos(lat2)*np.sin((lon2-lon1)/2)**2
    df["distance"] = (6371 * 2 * np.arcsin(np.sqrt(d))).astype("float32")

    df["avg_amt"] = df["amt"]
    df["std_amt"] = 0.0
    df["nb_trans"] = 1.0

    expected_cols = [
        'amt', 'hour', 'day_of_week', 'day', 'month', 'age', 'lat', 'long', 
        'city_pop', 'distance', 'avg_amt', 'std_amt', 'nb_trans', 
        'category', 'gender', 'state', 'merchant', 'job'
    ]
    return df[expected_cols]

# --- 5. ENDPOINT NETTOYÉ ---
@app.post("/predict")
def predict(data: Transaction):
    if model is None:
        return {"error": "Modèle non chargé"}
    try:
        X_processed = prepare_input(data.dict())
        prediction = model.predict(X_processed)
        
        # On ne renvoie que la valeur brute
        return int(prediction[0])
        
    except Exception as e:
        return {"error": str(e)}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=7860)

