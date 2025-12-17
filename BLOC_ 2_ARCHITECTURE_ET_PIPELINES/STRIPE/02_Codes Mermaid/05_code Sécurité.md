---
config:
  layout: elk
  theme: neo-dark
---
flowchart TD

    subgraph Clients
        Browser
        Mobile
    end

    subgraph Edge[EDGE Layer]
        WAF[Web Application Firewall]
        CDN[CDN + TLS Termination]
        BOT[Anti-Bot / Rate Limiting]
    end

    subgraph App[Application Layer]
        API[API Gateway]
        Auth[Identity / OAuth]
        Services[Microservices]
    end

    subgraph DataSecurity[Data Security Layer]
        KMS[Key Management System]
        Vault[Secrets Manager]
        DLP[Data Loss Prevention]
        IAM[RBAC/ABAC IAM Engine]
    end

    subgraph Storage[Data Storage Layer]
        OLTP[(OLTP DB)]
        NoSQL[(NoSQL / Features / Logs)]
        DWH[(Data Warehouse)]
        Lake[(Data Lake)]
    end

    Browser --> WAF --> API --> Services
    Mobile --> WAF
    Services --> OLTP
    Services --> NoSQL
    Services --> Lake
    Lake --> DWH

    KMS --> Storage
    Vault --> Services
    IAM --> Storage
    DLP --> Lake