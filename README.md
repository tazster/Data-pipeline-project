# Formula 1 Streaming Data Pipeline (Kafka + Postgres + Docker)

An end-to-end data engineering pipeline designed to stream, process, and stage Formula 1 racing telemetry and event data. The architecture orchestrates containerized API ingestion workers, an Apache Kafka message broker, and a PostgreSQL database layer to demonstrate a scalable, decoupled real-time data streaming design.

## 🏗️ Architecture Overview

The system is fully modularized and runs within an isolated Docker virtual network, decoupling configuration settings from the core application logic.

1. **Producer Layer (`F1DataStreamer`):** A Python worker that extracts structured racing data from REST endpoints. It acts as an event producer, streaming high-volume JSON data records onto specific Kafka topics.
2. **Streaming & Buffering Layer (Apache Kafka):** Handles high-throughput event virtualization. Messages are queued across distinct broker channels (`f1-meetings` and `f1-sessions`) to absorb data volume spikes and allow independent scaling.
3. **Consumer & Staging Layer (`F1DataConsumerPipeline`):** A downstream python engine that monitors Kafka topics, aggregates raw streaming payloads into optimized Pandas DataFrames, executes schema cleaning/timestamp transformations, and batches records into operational database targets.
4. **Storage Layer (PostgreSQL):** Serving as the reliable data warehouse target, housing structured tables live and prepared for downstream BI analytics or dashboarding.

## 🛠️ Tech Stack & Infrastructure
* **Language:** Python 3 (Pandas, Requests, SQLAlchemy, Kafka-Python)
* **Message Broker:** Apache Kafka (with Zookeeper orchestration)
* **Target Database:** PostgreSQL 
* **Containerization:** Docker & Docker Compose

## 🚀 Getting Started

### Prerequisites
* Docker and Docker Compose installed on your local machine.

### Installation & Run Instructions
1. Clone the repository:
   ```bash
   git clone [https://github.com/tazster/Data-pipeline-project.git](https://github.com/tazster/Data-pipeline-project.git)
   cd Data-pipeline-project
