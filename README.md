# Project Name
☁️ Azure End-to-End Data Engineering Pipeline

# 🚀 Overview
This project demonstrates a complete Azure Data Engineering pipeline using cloud-native services for ingestion, transformation, storage, and analytics.

**The pipeline includes three phases:**

1. Data Ingestion (ADF)
  * Fetch data from GitHub and push it dynamically to Azure using JSON configuration files.
  * Use Linked Services, Iteration, and Conditional activities to load data efficiently.
  * Create Bronze/Silver/Gold layers in Azure Data Lake Storage.
  * Git integration for version-controlled JSON pipelines.

2. Data Processing (Databricks)
  * Create a Databricks workspace and clusters for computation.
  * Set up service-level application via Microsoft Entra ID (App Registration) to securely access Data Lake.
  * Use PySpark to transform raw Bronze data into Silver layer.
  * Perform data visualizations directly in Databricks (bar charts, pie charts, etc.).

3. Analytics Layer (Synapse Analytics)
  * Create Synapse workspace and account.
  * Assign permissions via IAM / Managed Identity to access Silver layer in Data Lake.
  * Use serverless Lakehouse approach for tabular abstractions.
  * Create schemas, views, external file formats (Parquet) and master key for security.
  * Expose Gold layer for Power BI visualizations.



## 🏗️ Architecture
Source Data → Azure Data Factory (Ingestion)
             → Azure Data Lake (Raw Zone)
             → Azure Databricks / Synapse (Transformation)
             → Azure SQL Database / Synapse (Serving)
             → Power BI / Dashboard (Visualization)

## Core Azure Service Used 
1. Azure Data Factory (ADF) – Orchestration & ETL
2. Azure Data Lake Storage Gen2 – Data lake for raw & curated zones
3. Azure Databricks or Azure Synapse Analytics – Data processing and transformation
4. Azure SQL Database – Serving layer for analytics
5. Azure Monitor / Log Analytics – Monitoring and logging

## 💡 Key Features
* Dynamic ADF pipelines with JSON configuration
* Git integration for version-controlled pipeline JSONs
* Bronze, Silver, Gold Data Lake layers
* Secure access via Microsoft Entra ID App Registration
* Databricks-based data transformation and visualization
* Synapse Lakehouse tabular abstractions for analytics
* Power BI integration for dashboards

##  📸 Screenshots
1. Azure Data Lake Containers (Bronze, Silver, Gold)
<img width="1100" height="400" alt="Image" src="https://github.com/user-attachments/assets/c6934742-cd71-41de-8594-706ec80faa3d" />
2. Gold Layer – extsales Table
   


