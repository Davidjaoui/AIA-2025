flowchart LR
    %% Sources
    OLTP[(OLTP\nTransactions)]
    APP[(Mobile / Web)]
    LOGS[(Logs Apps & Services)]
    
    %% Bus
    KAFKA[(Kafka - Event Bus)]
    CDC[(CDC Connector)]
    
    %% Storage Layers
    NOSQL[(NoSQL\nClickstream / Logs / Features)]
    DATALAKE[(Data Lake\nRaw / Staged / Curated)]
    DWH[(DWH OLAP\nSnowflake / BigQuery)]
    
    %% Processing
    AIRFLOW[(Airflow\nBatch Jobs)]
    STREAM[(Stream Processing\nFlink / Spark)]

    %% ML
    FEATURESTORE[(Feature Store)]
    MLTRAIN[(ML Training)]
    MLPRED[(ML Inference)]
    
    %% BI
    BI[(Dashboards / Analytics)]

    %% Flows
    OLTP --> CDC --> KAFKA
    APP --> KAFKA
    LOGS --> KAFKA

    KAFKA --> STREAM --> NOSQL
    KAFKA --> STREAM --> DATALAKE

    AIRFLOW --> DATALAKE
    DATALAKE --> DWH
    AIRFLOW --> DWH

    NOSQL --> FEATURESTORE
    DWH --> FEATURESTORE

    FEATURESTORE --> MLTRAIN --> MLPRED
    MLPRED --> NOSQL

    DWH --> BI
