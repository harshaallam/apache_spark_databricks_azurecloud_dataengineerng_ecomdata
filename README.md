# Ecom data Data Engineering pipeline using Apache Spark on Databricks with Azure Cloud

Project Overview:
This project delves in dealing with large dataset and processes using Apache Spark with the help of Azure Databricks platform to extract useful business insights. An automated pipleine was built from extracting the dataset from ADLS to building dashboards using Azure Databricks.


Tools Used:
- Apache Spark
- Databricks
- Azure Databricks
- Azure Data Factory
- Azure Data Lake Storage

Azure servicese used: Azure Data Lake Storage: It is a highly scalable and secure data storage service built for big data analytics, providing hierarchical namespace and optimized for Azure-based data processing and analytics workloads. It allows storage of large volumes of structured and unstructured data for big data scenarios.

Azure Data Factory: It is a fully managed cloud-based ETL service that enables you to orchestrate and automate data workflows across different data sources, including ADLS, databases, and cloud storage. It is used to move and transform data at scale, making it available for analytics and business insights.

Azure Databricks: It is an Apache Spark-based analytics platform that integrates with Azure services to offer a collaborative environment for data engineering, machine learning, and big data analytics. It can read and process data from ADLS, transforming and analyzing large datasets efficiently and collaboratively.

Dataset:
The dataset contains ecommerce data about fashion business operated accross the globe. 
The dataset is available at: https://data.world/jfreex/e-commerce-users-of-a-french-c2c-fashion-store

Chunking the data:
- The dataset contains users, buyers, countries, sellers, and data.
- Among all of the categories users is the only data that gets changed, and rest all of the data remains same.
- The users data is keep on generating based on newly added users, and the rest of the data rarely changes based on slowly chnaging dimesion concept.
- The users data is huge and therfore chunked the data into 10 chunked datasets. Refer `users_chunk_data.ipynb` which has the code to chunk users data, and chunked data can be found at `chunked_data/`
- The users data is chunked for the ease of processing.







