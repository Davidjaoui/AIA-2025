erDiagram

    CUSTOMER {
        string customer_id PK
        string email
        string phone
        string created_at
        string country_code
    }

    MERCHANT {
        string merchant_id PK
        string name
        string category
        string country_code
        string created_at
    }

    PAYMENT_METHOD {
        string payment_method_id PK
        string customer_id FK
        string type
        string token
        string created_at
    }

    TRANSACTION {
        string transaction_id PK
        string merchant_id FK
        string customer_id FK
        string payment_method_id FK
        float amount
        string currency
        string status
        string device_type
        string ip_address
        string created_at
        float fraud_score
    }

    REFUND {
        string refund_id PK
        string transaction_id FK
        float amount
        string status
        string created_at
    }

    DISPUTE {
        string dispute_id PK
        string transaction_id FK
        string reason
        string status
        string created_at
    }

    COUNTRY {
        string country_code PK
        string name
        string region
    }

    CURRENCY_RATE {
        string currency PK
        float rate_to_usd
        string updated_at
    }

    %% RELATIONS
    CUSTOMER ||--o{ PAYMENT_METHOD : "has"
    CUSTOMER ||--o{ TRANSACTION : "initiates"
    MERCHANT ||--o{ TRANSACTION : "receives"

    TRANSACTION ||--o{ REFUND : "may generate"
    TRANSACTION ||--o{ DISPUTE : "may trigger"

    PAYMENT_METHOD ||--o{ TRANSACTION : "used for"

    COUNTRY ||--o{ CUSTOMER : "geo"
    COUNTRY ||--o{ MERCHANT : "geo"

    CURRENCY_RATE ||--o{ TRANSACTION : "conversion"
