# 🛡️ Projet : Détection Automatisée de Fraude Bancaire

## 📌 Présentation
La fraude représente un défi financier colossal, s'élevant à plus d'un milliard d'euros par an rien que dans l'Union Européenne. 

L'objectif de ce projet est d'utiliser l'Intelligence Artificielle non seulement pour identifier ces fraudes avec précision, mais surtout pour **réagir en temps réel**. Le véritable défi technique ici est le passage de l'expérimentation (Notebook) à la mise en production réelle pour traiter des flux de transactions continus.

## 🏗️ Architecture du Pipeline de Données
Le concept repose sur une infrastructure robuste qui sépare la collecte, l'orchestration et le stockage.



### 1. Collecte & Flux (Real-time)
* **Source :** Les données proviennent d'une API de paiement en temps réel.
* **Fréquence :** Mise à jour chaque minute, avec une transaction générée toutes les 12 secondes.

### 2. Modèle & Intelligence
* **Modèle :** Utilisation de l'algorithme **Random Forest** pour sa simplicité, sa stabilité et sa facilité de mise à disposition via API.
* **Gestion du cycle de vie :** L'entraînement et le versioning du modèle sont gérés dans **MLFlow**.
* **Conteneurisation :** Isolation complète de l'environnement avec **Docker**.

### 3. Orchestration (Airflow)
L'outil **Airflow** automatise le script de collecte et gère le processus ETL/ELT via deux DAGs :
* **DAG de Prédiction :** Extraction et prédiction toutes les 12 secondes avec stockage immédiat dans **Neon DB**.
* **DAG de Reporting :** Envoi automatique d'un e-mail récapitulant les fraudes détectées à $J-1$.

## 🛠️ Stack Technique
* **Langage :** Python
* **Orchestration :** Apache Airflow
* **Machine Learning :** Scikit-Learn, MLFlow
* **Base de données :** Neon DB (PostgreSQL Cloud)
* **Infrastructure :** Docker

## 🎯 Besoins métiers satisfaits
* **Continuité :** Modèle accessible en continu pour le temps réel.
* **Automatisation :** Orchestration automatique pour une détection immédiate.
* **Audit & Analyse :** Stockage des prédictions et des logs pour permettre l'audit et le reporting.
* **Traçabilité :** Conservation des modèles et des données pour assurer le versioning et les analyses futures.

## 🚀 Installation

1. **Cloner le projet**
   ```bash
   git clone [https://github.com/votre-nom/fraud-detection-airflow.git](https://github.com/votre-nom/fraud-detection-airflow.git)
   cd fraud-detection-airflow
2. **Configuration**
Créez un fichier .env à la racine pour vos identifiants :

- NEON_DB_URL
- SMTP_SERVER / EMAIL_USER / EMAIL_PASSWORD

3. **Lancement avec Docker**
dans le terminal: 
docker-compose up -d