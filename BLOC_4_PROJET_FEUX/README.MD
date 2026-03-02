# 🔥 Projet de Prévision du Risque d’Incendie

Ce projet a pour objectif de développer un système complet permettant d’analyser, prédire et surveiller le risque d’incendie sur un territoire donné.  
Il s’appuie sur une chaîne de traitement complète : ingestion des données, analyse exploratoire, modélisation, automatisation des workflows, suivi des expériences et déploiement.

---

## 📁 Arborescence du projet

```bash
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

```


---

## 📦 Description des dossiers

### **Airflow/**
Contient les DAGs et workflows d’automatisation du pipeline de données :
- ingestion des données météo et environnementales  
- prétraitement et enrichissement  
- exécution régulière des prédictions  
- génération de rapports  

### **all_test/**
Scripts historiques et ressources de tests complémentaires.

### **api/**
Code source de l’API permettant de :
- exposer les prédictions du modèle  
- interroger le système en temps réel  
- intégrer l’outil dans une application externe  

### **EDA/**
Exploratory Data Analysis :
- visualisations  
- statistiques descriptives  
- détection des corrélations  
- nettoyage et préparation initiale des données  

### **jenkins/**
Pipelines CI/CD utilisés pour automatiser :
- les tests  
- le déploiement du modèle  
- le suivi des versions  

### **mlflow-projet/**
Suivi des expériences, métriques et artefacts :
- entraînement des modèles  
- comparaison des versions  
- gestion du registre de modèles  

### **Modèles/**
Modèles entraînés, scripts d’entraînement et artefacts associés.  
Inclut les différentes approches testées (ML, DL, etc.).

### **Notebook_autre_dep/**
Notebooks exploratoires pour d’autres départements / zones géographiques.

### **Notebook_Corse/**
Notebooks spécifiques aux analyses réalisées sur les données de la Corse (scénarios, tests, cartographies).

### **projet-feux/**
Dossier central du projet incendie :
- préparation des jeux de données  
- extraction des indicateurs météo et environnementaux  
- features engineering  
- scripts spécifiques aux incendies  

### **tests/**
Tests unitaires et d’intégration :
- validation du code  
- robustesse des modules  
- fiabilité du pipeline complet  

---

## 🎯 Objectifs du projet

- Prévoir le **niveau de risque d’incendie** à différentes échelles.  
- Construire une pipeline complète et automatisée.  
- Suivre et comparer les modèles grâce à MLflow.  
- Proposer une API pour exposer les résultats.  
- Étendre le système à différentes régions (Corse, autres départements…).  

---

## 🚀 Technologies principales

- Python, Pandas, Scikit-learn, MLflow  
- Airflow, Jenkins, API REST (FastAPI ou Flask)  
- Jupyter Notebooks  
- Matplotlib, Seaborn  
