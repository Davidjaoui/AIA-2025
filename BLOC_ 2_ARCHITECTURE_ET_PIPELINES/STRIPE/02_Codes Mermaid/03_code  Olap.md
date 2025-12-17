flowchart TB

    %% Collections principales
    CLICKSTREAM["Clickstream Events\n(collection)"]
    SESSION["User Sessions\n(collection)"]
    LOGS["System Logs\n(collection)"]
    FEATURES["ML Feature Store\n(collection)"]
    FRAUD_EVENTS["Fraud Events\n(collection)"]
    CUSTOMER_REF["Customer Reference\n(view / mirror)"]
    TRANSACTION_REF["Transaction Reference\n(view / mirror)"]

    %% Relations logiques
    SESSION --> CLICKSTREAM
    CUSTOMER_REF --> SESSION
    CUSTOMER_REF --> FEATURES
    TRANSACTION_REF --> FRAUD_EVENTS
    FEATURES --> FRAUD_EVENTS
    CLICKSTREAM --> FEATURES

    %% Intégration
    OLTP[(OLTP\nTransactions)] --> TRANSACTION_REF
    OLTP --> CUSTOMER_REF
    CDC[(CDC Stream)] --> FEATURES
    CDC --> FRAUD_EVENTS
    CDC --> CLICKSTREAM
