---
config:
  layout: fixed
  theme: neo-dark
---
flowchart LR
    Sources(["<b>SOURCES DE DONNES</b><br>API Stripe<br>Applications Web / Mobile<br>Systèmes Tiers"]) ==> OLTP(("<b>COUCHE OLTP</b><br>Traitement Transactionnel<br>Base OLTP Transactions ACID<br>Kafka Topics CDC<br>Replica Lecture<br>Redis Cache"))
    OLTP ==> Ingestion@{ label: "<b>COUCHE D'INGESTION TEMPS REEL</b><br>Kafka Connect CDC<br>Apache Kafka Streaming<br>Apache Flink-Stream Processing" } & DL(["<b>DATA LAKE- STOCKAGE CENTRALISE</b><br>S3 / ADSL - Object Storage<br>Bronze/Silver/Gold"]) & ETL(["<b>COUCHE ETL / ELT</b><br>Apache Airflow Orchestration<br>dbt Transformations<br>Spark Jobs Batch Processing"])
    Ingestion ==> DL & NoSQL(["<b>COUCHE NoSQL</b><br>Données non structurées<br>MongoDB docs JSON<br>Elasticsearch Logs &amp; Recherche<br>Cassandra Séries Temporelles<br>ML Feature Store"])
    DL ==> NoSQL & ETL
    ETL ==> OLAP(["<b>COUCHE OLAP - Analytique</b><br>Snowflake/Redshift Data Warehouse<br>OLAP Cubes<br>Vues Matérialisées"])
    OLAP ==> Pres(["<b>COUCHE DE PRESENTATION</b><br>API Analytics<br>Dashboards BI tableau/looker/Power BI<br>Rapports de Conformité<br>Chatbot IA Support Client"]) & ML(["<b>COUCHE ML &amp; ANALYTICS</b><br>Detection Fraude - realTime ML<br>Analyses Prédictives - ML Models<br>Segmentation Client - Analytics<br>LLM Fine Tunning <br>Support Client"])
    ML ==> Pres
    Sec(["<b>SECURITE CONFORMITE</b><br>Audit Logs<br>Chiffrement TDE/SSL<br>Masquage de Données<br>RBAC &amp; IAM"]) ==> OLTP & NoSQL & OLAP & DL
    NoSQL ==> ETL & ML
    MLOps(["<b>ML Ops</b>"]) <==> ML
    MLOps ==> DL

    Ingestion@{ shape: stadium}
    style Sources fill:#BBDEFB,color:#000000
    style OLTP fill:#1565c0,color:#FFFFFF
    style Ingestion fill:#FFF9C4,color:#000000
    style DL fill:#FFFFFF,color:#000000,stroke:#757575
    style ETL fill:#FFD600,color:#000000,stroke:#FFF9C4
    style NoSQL fill:#2e7d32,color:#FFFFFF,stroke:#00C853
    style OLAP fill:#FF6D00,color:#000000,stroke:#FF6D00
    style Pres fill:#00acc1,color:#000000
    style ML fill:#ab47bc,color:#000000
    style Sec fill:#d50000,color:#fff,stroke:#D50000
    style MLOps fill:#757575,color:#FFFFFF
    linkStyle 0 stroke:#BBDEFB,fill:none
    linkStyle 1 stroke:#2962FF,fill:none
    linkStyle 2 stroke:#2962FF,fill:none
    linkStyle 3 stroke:#2962FF,fill:none
    linkStyle 4 stroke:#FFF9C4,fill:none
    linkStyle 5 stroke:#FFF9C4,fill:none
    linkStyle 6 stroke:#FFFFFF,fill:none
    linkStyle 7 stroke:#FFFFFF,fill:none
    linkStyle 8 stroke:#FFD600,fill:none
    linkStyle 9 stroke:#FF6D00,fill:none
    linkStyle 10 stroke:#FF6D00,fill:none
    linkStyle 11 stroke:#AA00FF,fill:none
    linkStyle 12 stroke:#D50000,fill:none
    linkStyle 13 stroke:#D50000,fill:none
    linkStyle 14 stroke:#D50000,fill:none
    linkStyle 15 stroke:#D50000,fill:none
    linkStyle 16 stroke:#00C853,fill:none
    linkStyle 17 stroke:#00C853,fill:none
    linkStyle 18 stroke:#757575,fill:none
    linkStyle 19 stroke:#757575,fill:none