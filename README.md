# Project Apache Spark in Scala

The main purpose of the project is to practice Apache Spark in Scala.

## Architecture

![scala_project drawio](https://github.com/skalskibukowa/Project-Spark-Scala-ETL/assets/29678557/64cdd17d-6080-4fa7-a11b-755ec3771f83)

## Overview

Project Overview:

This project leverages Scala to implement an Extract, Transform, and Load (ETL) pipeline. Data is extracted from various sources (CSV files and PostgreSQL databases), undergoes transformations and analysis, and is then loaded into three distinct sinks (CSV, Parquet, and PostgreSQL).

**Data Sources:**

Multiple CSV files
PostgreSQL databases:
Transaction Poland
Transaction France
Transaction China
Transaction USA

**Data Transformations and Analysis:**

The specific transformations and analysis steps are not explicitly mentioned in the image or description. However, the project likely involves data cleaning, filtering, aggregation, and potentially more complex operations depending on the data's nature and intended use.

**Data Sinks:**

CSV files
Parquet files
PostgreSQL databases

## Instruction for building and running Scala application

1. Copy the project from GitHub
2. Open project
3. Build "postgres" Docker image
cd PostgresSQL && docker build -t postgres .
4. Start the Docker container
5. Check PostgreSQL connection:
docker exec -it postgres psql -U postgres -d postgres \dt
6. Run Scala application
