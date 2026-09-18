# Covid19_ADF_Azure_project | Data Engineering Azure Project

## Introduction
This project implements an end-to-end data engineering pipeline on Azure to ingest, process, and report on COVID-19 data. It combines case, death, hospital admissions, and testing data published by the ECDC (European Centre for Disease Prevention and Control) with population reference data, producing clean, analysis-ready datasets and an interactive Power BI dashboard.

## Architecture
![Project Architecture](architecture_image.png)

## Technology Used

1. Cloud & Data Engineering
   -  Azure Data Factory
   -  Azure Data Lake Storage Gen2 (ADLS Gen2)
   -  Azure Blob Storage
   -  Azure Databricks
   -  Apache Spark
   -  Azure SQL Database
   -  Power BI

2. Development & Engineering
   - Python / PySpark
   - SQL
   - Git
   - Azure DevOps
   - ADF Data Flows
   - REST/HTTP data ingestion

ADF is used for ingestion, orchestration, scheduling, monitoring, and pipeline dependencies. Azure Databricks provides Spark-based processing for scalable transformation workloads.

## Dataset Used
ECDC COVID-19 datasets (cases & deaths, hospital & ICU admissions, testing rates, country response measures) — publicly published by the European Centre for Disease Prevention and Control, ingested via HTTP connector
Population by age data — reference dataset (population_by_age.tsv.gz), sourced from Eurostat, loaded from Azure Blob Storage

### More Info About Dataset
1. Population by age data- [population_by_age.tsv.gz](population_by_age.tsv.gz)
2. ECDC datasets
    - [cases_deaths.csv](cases_deaths.csv)
    - [hospital_admissions.csv](hospital_admissions.csv)
    - [country_response.csv](country_response.csv)
    - [testing.csv](testing.csv)
   

## Data Model Diagram
![datamodel.png](datamodel.png)

## Data ingestion pipeline

<img width="1000" height="750" alt="image" src="https://github.com/user-attachments/assets/d998bbb1-1ffd-4270-99f8-cb8ddf1def9e" />




## Data Flow transformation workflow

<img width="1000" height="400" alt="image" src="https://github.com/user-attachments/assets/a75cf673-a8a8-4f5f-87b3-45654dad57ae" />


## Transformation (PySpark files)
1. 



