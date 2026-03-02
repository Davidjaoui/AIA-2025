# 📂 Portfolio de projets – Certification AIA

Ce dépôt contient l’ensemble des projets réalisés dans le cadre de la **Certification RNCP38777 – Niveau 7 – Architecte en Intelligence Artificielle**.

---

## 🗂️ Liste des projets

### 🔹 Bloc 1 : Concevoir et piloter la gouvernance des données
- **Projet Spotify** – Data Gouvernance

### 🔹 Bloc 2 : Concevoir et déployer des architectures de données
- **Projet Stripe** – From SQL to NoSQL

### 🔹 Bloc 3 : Concevoir et mettre en oeuvre des pipelines de données 
- **Projet Automatic Fraud Detection** – Workflow Orchestration

### 🔹 Bloc 4 : Construire, déployer et piloter des solutions d'IA
- **Projet Final** – Prévision de risque incendie

---

## 📁 Structure du repository

```bash

.
├── 01_BLOC_1/
│   └── SPOTIFY/
│       ├── livrables/
│       └── presentation/
│
├── 02_BLOC_2/
│   └── STRIPE/
│       ├── schemas/
│       ├── documentation/
│       ├── exemples de requêtes/
│       └── presentation/
│
├── 03_BLOC_3/
│   ├── airflow/
│   │   ├── airflow_env/              
│   │   ├── dags/                     
│   │   ├── logs/                     
│   │   ├── docker-compose/           
│   │   └── Dockerfile/ 
│   │ 
│   ├── api_fraud/    
│   │   ├── .git/
│   │   ├── Dockerfile/
│   │   ├── app.py/
│   │   └── requirements.txt/
│   │ 
│   ├── api_real-time-paiements/
│   │   ├── Dockerfile
│   │   ├── app.py
│   │   └── run.sh
│   │
│   ├── Dashboard-fraud-detection/
│   │   ├── .git/
│   │   ├── src/
│   │   ├── Dockerfile
│   │   └── requirements.txt
│   │
│   ├── fraud-detection/
│   │   ├── modèle/
│   │   ├── .git/
│   │   ├── start_mlflow.sh
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   ├── encoders_historisés/
│   │   └── predict_examples/
│   │
│   ├── modèle_xgboost/               
│   └── tests/   
│
├── 04_BLOC_4/
│   └── src/
│       ├── Airflow/
│       ├── all_test/
│       ├── api/
│       ├── EDA/
│       ├── jenkins/
│       ├── mlflow-projet/
│       ├── Modèles/
│       ├── Notebook_autre_dep/
│       ├── Notebook_Corse/
│       ├── projet-feux/
│       └── tests/
└── README.md



Ce projet est libre d’utilisation, de modification et de distribution.









