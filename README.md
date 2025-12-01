# ShopPulse: Real-Time E-Commerce Data Lakehouse 🚀

## 📖 Project Overview
ShopPulse is a scalable, real-time data engineering platform designed to process high-velocity e-commerce clickstream data. It utilizes a **Kappa Architecture** pattern to ingest, process, and serve insights with sub-minute latency.

The system handles end-to-end data flow:
1.  **Data Generation:** Simulating user traffic (Views, Carts, Purchases).
2.  **Ingestion:** Buffering events via **Apache Kafka**.
3.  **Processing:** Stateful stream processing with **Apache Spark** (Structured Streaming).
4.  **Storage:** Creating a **Silver Layer** Data Lake using **Parquet** format.
5.  **Analytics:** Serving real-time metrics via **DuckDB** (Zero-Copy Architecture).

---

## 🛠 Tech Stack
| Component | Technology | Role |
| :--- | :--- | :--- |
| **Containerization** | Docker & Docker Compose | Infrastructure as Code (IaC) |
| **Ingestion** | Apache Kafka + Zookeeper | Message Broker & Decoupling |
| **Processing** | Apache Spark (PySpark) | Distributed Stream Processing |
| **Storage** | Local FS (Simulating S3) | Data Lake (Parquet Format) |
| **Serving** | DuckDB | High-performance OLAP Engine |
| **Language** | Python 3.9 | Application Logic |

---

## 🏗 Architecture

```mermaid
graph TD
    subgraph Source
        A[Python Generator] -->|JSON Events| B(Apache Kafka)
    end
    
    subgraph Processing
        B -->|Stream| C{Spark Structured Streaming}
        C -->|Parse & Clean| C
    end
    
    subgraph Storage_Lakehouse
        C -->|Write Append| D[(Silver Layer / Parquet)]
        D -.->|Checkpointing| E[Fault Tolerance]
    end
    
    subgraph Serving_Layer
        D -->|Read on Demand| F(DuckDB)
        F -->|Aggregates| G[Real-Time Dashboard]
    end