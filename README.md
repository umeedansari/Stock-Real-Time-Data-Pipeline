# Real-Time Stock Market Data Pipeline | Modern Data Stack

This project demonstrates an end-to-end real-time data pipeline using the Modern Data Stack. We capture live stock market data from an external API, stream it in real time, orchestrate transformations, and deliver analytics-ready insights — all in one unified project.

## ⚡ Tech Stack

- **Snowflake** → Cloud Data Warehouse
- **DBT** → SQL-based Transformations
- **Apache Airflow** → Workflow Orchestration
- **Apache Kafka** → Real-time Streaming
- **Python** → Data Fetching & API Integration
- **Docker** → Containerization

## ✅ Key Features

- Fetching live stock market data (not simulated) from Finnhub API
- Real-time streaming pipeline with Kafka
- Orchestrated ETL workflow using Airflow
- Transformations using DBT inside Snowflake
- Scalable cloud warehouse powered by Snowflake



## 🚀 Getting Started

1. Clone this repository and set up environment
2. Start Kafka + Airflow services via Docker
3. Run the Python producer to fetch live stock data
4. Data flows into Snowflake → DBT applies transformations
5. Orchestrate everything with Airflow
6. Connect Power BI for visualization

## ⚙️ Step-by-Step Implementation

### 1. Kafka Setup

Configured Apache Kafka locally using Docker. Created a `stocks-topic` to handle live stock market events with defined producers (API fetch) and consumers (pipeline ingestion).

### 2. Live Market Data Producer

Developed Python producer script `producer.py` to fetch real-time stock prices from the Finnhub API using an API key. Streams stock data into Kafka in JSON format.


### 3. Kafka Consumer → MinIO

Built Python consumer script `consumer.py` to consume streaming data from Kafka. Stored consumed data into MinIO buckets (S3-compatible storage) organized into folders for raw/bronze layer ingestion.


### 4. Airflow Orchestration

Initialized Apache Airflow in Docker. Created DAG (`minio-to-snowflake.py`) to:
- Load data from MinIO into Snowflake staging tables (Bronze)
- Schedule automated runs every 1 minute

### 5. Snowflake Warehouse Setup

Created Snowflake database, schema, and warehouse. Defined staging tables for Bronze → Silver → Gold layers.

### 6. DBT Transformations

Configured DBT project with Snowflake connection. Models include:

- Bronze models → Raw structured data
- Silver models → Cleaned, validated data
- Gold models → Analytical views (Candlestick, KPI, Tree Map)


## 📊 Final Deliverables

- ✅ Automated real-time data pipeline
- ✅ Snowflake tables (Bronze → Silver → Gold)
- ✅ Transformed analytics models with DBT
- ✅ Orchestrated DAGs in Airflow

## 🔧 Prerequisites

- Docker and Docker Compose
- Python 3.8+
- Snowflake account
- Finnhub API key

## 📌 Architecture
<img width="1212" height="692" alt="image" src="https://github.com/user-attachments/assets/ea2c7e7e-b667-452b-96e1-4ea7c04b76b3" />


