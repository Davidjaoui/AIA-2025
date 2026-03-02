from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
import io
import json
import psycopg2
import time

# --- CONFIGURATION ---

from airflow.models import Variable # Import indispensable

# On récupère les valeurs dynamiquement
# Si la variable n'existe pas, on met une chaîne vide par défaut
NEON_CONN_STRING = Variable.get("NEON_CONN_STRING")
URL_JEDHA = Variable.get("URL_JEDHA")
URL_MON_API = Variable.get("URL_MON_API")



def process_iteration():
    # 1. EXTRACT
    response = requests.get(URL_JEDHA)
    data_json = response.json()
    if isinstance(data_json, str): data_json = json.loads(data_json)
    
    df = pd.read_json(io.StringIO(json.dumps(data_json)), orient="split")
    record = df.iloc[0].to_dict()
    
    # Nettoyage des dates pour le JSON
    for key, value in record.items():
        if isinstance(value, pd.Timestamp):
            record[key] = value.strftime("%Y-%m-%d %H:%M:%S")

    # 2. PREDICT
    record['trans_date_trans_time'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    pred_response = requests.post(URL_MON_API, json=record)
    res_api = pred_response.json()
    prediction = res_api.get('prediction') if isinstance(res_api, dict) else res_api

    # 3. STORE (Mapping complet pour éviter les NULL)
    conn = psycopg2.connect(NEON_CONN_STRING)
    cur = conn.cursor()
    
    # On prépare TOUTES les colonnes demandées
    valeurs = (
        record.get('trans_date_trans_time'),
        record.get('cc_num'),
        float(record.get('amt', 0)),
        record.get('lat'),
        record.get('long'),
        record.get('city_pop'),
        record.get('dob'),
        record.get('unix_time'),
        record.get('merch_lat'),
        record.get('merch_long'),
        int(prediction),
        record.get('merchant'),
        record.get('category'),
        record.get('trans_num'),
        record.get('first'),  # Ajouté
        record.get('last'),   # Ajouté
        record.get('gender'), # Ajouté
        record.get('street'), # Ajouté
        record.get('city'),   # Ajouté
        record.get('state'),  # Ajouté
        record.get('zip'),    # Ajouté
        record.get('job')     # Ajouté
    )
    
    query = """
        INSERT INTO fraud_logs (
            trans_date_trans_time, cc_num, amt, lat, long, 
            city_pop, dob, unix_time, merch_lat, merch_long, 
            prediction, merchant, category, trans_num,
            first, last, gender, street, city, state, zip, job
        ) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    cur.execute(query, valeurs)
    conn.commit()
    cur.close()
    conn.close()
    print(f"✅ Succès : Transaction {record.get('trans_num')} insérée pour {record.get('first')} {record.get('last')}")

def run_realtime_loop():
    # Boucle pour simuler du quasi temps réel (5 x 12s = 60s)
    for i in range(5):
        try:
            process_iteration()
        except Exception as e:
            print(f"❌ Erreur lors de l'itération {i+1}: {e}")
        
        if i < 4:
            time.sleep(12)

# --- CONFIGURATION DAG ---
with DAG(
    'dag_jedha_realtime_fraud',
    default_args={'owner': 'david', 'retries': 0},
    schedule='* * * * *', 
    start_date=datetime(2024, 1, 1),
    catchup=False
) as dag:

    main_task = PythonOperator(
        task_id='process_12s_cycle',
        python_callable=run_realtime_loop
    )