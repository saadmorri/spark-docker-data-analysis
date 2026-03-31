# Spark Data Analysis with Docker

## 🚀 Project Overview
This project demonstrates how to run Apache Spark locally using Docker and perform large-scale data analysis on a real-world dataset.

The analysis is implemented using:
- Spark DataFrame API
- Spark SQL

## 🛠 Technologies Used
- Apache Spark
- PySpark
- Docker & Docker Compose
- Python

## Dataset
NYC 311 Service Requests dataset.

## How to run

```bash
docker-compose up -d
docker exec -it spark_assignment-spark-1 /opt/spark/bin/spark-submit /jobs/analysis.py