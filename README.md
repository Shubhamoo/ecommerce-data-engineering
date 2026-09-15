# E-commerce Data Engineering Project

## 📌 Project Overview

This project is an end-to-end **E-commerce Data Engineering pipeline**
built on Microsoft Azure and Azure Databricks.

The objective is to ingest raw e-commerce user data, process and
transform it using Apache Spark/PySpark, store the data in a layered
architecture, and build an analytics dashboard for business insights.

The project demonstrates a production-style cloud data engineering
workflow using **Azure Data Factory, Azure Data Lake Storage Gen2, Azure
Databricks, Delta Lake, and Databricks SQL**.

------------------------------------------------------------------------

## 🏗️ Architecture

``` text
                    ┌──────────────────────┐
                    │   Raw CSV Files      │
                    │  E-commerce Dataset  │
                    └──────────┬───────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │ Azure Data Lake      │
                    │ Storage Gen2          │
                    │ landing-zone-1       │
                    └──────────┬───────────┘
                               │
                       Storage Event Trigger
                               │
                               ▼
                    ┌──────────────────────┐
                    │ Azure Data Factory   │
                    │      Pipeline        │
                    └──────────┬───────────┘
                               │
                         Copy Activity
                               │
                               ▼
                    ┌──────────────────────┐
                    │ Azure Data Lake      │
                    │ landing-zone-2       │
                    │ Raw/Processed Data   │
                    └──────────┬───────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │ Azure Databricks     │
                    │ PySpark Processing   │
                    └──────────┬───────────┘
                               │
                         Transformations
                               │
                               ▼
                    ┌──────────────────────┐
                    │ Silver Delta Layer  │
                    │ Cleaned User Data   │
                    └──────────┬───────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │ Databricks SQL       │
                    │ Analytics Warehouse  │
                    └──────────┬───────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │ AI/BI Dashboard       │
                    │ User Analytics        │
                    └──────────────────────┘
```

------------------------------------------------------------------------

## 🛠️ Technologies Used

  Technology                         Purpose
  ---------------------------------- ------------------------------------
  **Microsoft Azure**                Cloud platform
  **Azure Data Lake Storage Gen2**   Data storage
  **Azure Data Factory**             Data ingestion and orchestration
  **Azure Databricks**               Distributed data processing
  **Apache Spark / PySpark**         Data transformation
  **Delta Lake**                     Reliable analytical storage
  **Unity Catalog**                  Data governance and access control
  **Databricks SQL Warehouse**       SQL analytics
  **Databricks AI/BI Dashboard**     Data visualization
  **Git & GitHub**                   Version control

------------------------------------------------------------------------

## 📂 Project Structure

``` text
ecommerce-data-engineering/
│
├── databricks/
│   └── notebooks/
│       ├── Bronze_Layer.ipynb
│       ├── Silver_Layer.ipynb
│       ├── Gold_Layer.ipynb
│       └── process_ecommerce_user_data.ipynb
│
├── .gitignore
│
└── README.md
```

------------------------------------------------------------------------

## 🔄 Data Pipeline

### 1. Data Ingestion

Raw CSV files are uploaded to Azure Data Lake Storage Gen2:

``` text
landing-zone-1/
├── buyers-raw-1/
├── countries-raw-1/
├── sellers-raw-1/
└── user-raw-1/
```

Azure Data Factory monitors the `user-raw-1/` folder using a **Storage
Event Trigger**.

When a new CSV file arrives, the pipeline starts automatically.

------------------------------------------------------------------------

### 2. Azure Data Factory

The `ecom-user-data` pipeline contains two main activities:

``` text
Storage Event
     ↓
User_Data
     ↓
Process_User_Data
```

**User_Data**

Copies the incoming CSV file from the landing zone and converts it into
Parquet format.

**Process_User_Data**

Triggers the Databricks processing job and passes the generated file
name as a parameter.

Example:

``` text
chunk6.csv
    ↓
chunk6.parquet
```

This makes the pipeline event-driven instead of requiring manual
execution.

------------------------------------------------------------------------

## ⚡ Azure Databricks Processing

The main processing notebook is:

``` text
process_ecommerce_user_data.ipynb
```

The notebook accepts an `input_file` parameter from Azure Data Factory.

Example:

``` python
dbutils.widgets.text("input_file", "")

input_file = dbutils.widgets.get("input_file")
```

The notebook then:

1.  Reads the Parquet file.
2.  Applies schema and data type transformations.
3.  Handles null values.
4.  Converts columns to appropriate data types.
5.  Writes data to the Silver Delta layer.
6.  Performs Delta Lake MERGE/upsert logic.
7.  Moves the processed file to the processed-data folder.

------------------------------------------------------------------------

## 🥉 Bronze Layer

The Bronze layer represents the raw/ingested data.

Typical responsibilities:

-   Store incoming data with minimal transformation.
-   Preserve source data.
-   Convert source files into analytics-friendly formats.
-   Provide a reliable starting point for downstream processing.

Notebook:

``` text
Bronze_Layer.ipynb
```

------------------------------------------------------------------------

## 🥈 Silver Layer

The Silver layer contains cleaned and transformed user data.

Notebook:

``` text
Silver_Layer.ipynb
```

Example transformations include:

``` python
userDF = userDF.withColumn(
    "hasanyapp",
    col("hasAnyApp").cast("boolean")
)

userDF = userDF.withColumn(
    "socialNbFollowers",
    col("socialNbFollowers").cast(IntegerType())
)

userDF = userDF.withColumn(
    "productsPassRate",
    col("productsPassRate").cast(DecimalType(10, 2))
)
```

Null handling is also applied where required:

``` python
userDF = userDF.withColumn(
    "daysSinceLastLogin",
    when(
        col("daysSinceLastLogin").isNotNull(),
        col("daysSinceLastLogin").cast(IntegerType())
    ).otherwise(0)
)
```

The Silver data is stored as **Delta Lake**.

------------------------------------------------------------------------

## 🥇 Gold Layer

The Gold layer is designed for analytics and business-facing datasets.

Notebook:

``` text
Gold_Layer.ipynb
```

It provides data suitable for reporting, analytical queries, and
dashboarding.

------------------------------------------------------------------------

## 🔐 Unity Catalog & Security

The project uses **Unity Catalog** for data access and governance.

An Azure Databricks Access Connector with a managed identity is used to
access ADLS Gen2.

The managed identity is assigned:

``` text
Storage Blob Data Contributor
```

at the required storage-account scope.

A Unity Catalog storage credential and external locations are used to
access:

``` text
abfss://landing-zone-1@ecomadls.dfs.core.windows.net/
```

and

``` text
abfss://landing-zone-2@ecomadls.dfs.core.windows.net/
```

This avoids hardcoding cloud storage credentials inside notebooks.

------------------------------------------------------------------------

## 📊 Analytics & Dashboard

A Databricks SQL Warehouse is used to query the Silver Delta data.

The main analytical table is:

``` sql
default.ecommerce_users
```

It is created over the Silver Delta location:

``` sql
CREATE TABLE IF NOT EXISTS default.ecommerce_users
USING DELTA
LOCATION 'abfss://landing-zone-2@ecomadls.dfs.core.windows.net/silver/users/';
```

### Key Metrics

The dataset contains approximately:

``` text
Total Users        : 103,062
Users With Any App : 26,174
Android Users      : 4,819
iOS Users          : 21,527
```

### Dashboard Insights

The dashboard includes:

-   Total Users KPI
-   Users With Any App KPI
-   Android Users KPI
-   iOS Users KPI
-   Top 10 Countries by Users
-   App Adoption by Country (%)

Example analytical query:

``` sql
SELECT
    country,
    COUNT(*) AS total_users
FROM default.ecommerce_users
GROUP BY country
ORDER BY total_users DESC
LIMIT 10;
```

App adoption analysis:

``` sql
SELECT
    country,
    COUNT(*) AS total_users,
    SUM(
        CASE
            WHEN hasanyapp = true THEN 1
            ELSE 0
        END
    ) AS app_users,
    ROUND(
        SUM(
            CASE
                WHEN hasanyapp = true THEN 1
                ELSE 0
            END
        ) * 100.0 / COUNT(*),
        2
    ) AS app_adoption_percentage
FROM default.ecommerce_users
GROUP BY country
HAVING COUNT(*) >= 100
ORDER BY app_adoption_percentage DESC;
```

------------------------------------------------------------------------

## 🔁 End-to-End Automation

The final workflow is completely event-driven:

``` text
New CSV File
     ↓
ADLS Gen2 - landing-zone-1
     ↓
Storage Event Trigger
     ↓
Azure Data Factory
     ↓
Copy Data Activity
     ↓
Parquet File
     ↓
Databricks Job
     ↓
PySpark Transformation
     ↓
Delta MERGE / Upsert
     ↓
Silver Delta Layer
     ↓
Databricks SQL
     ↓
AI/BI Dashboard
```

A test file such as:

``` text
chunk6.csv
```

successfully flows through the entire pipeline and produces:

``` text
chunk6.parquet
```

in the processed-data layer while updating the Silver Delta table.

------------------------------------------------------------------------

## 🎯 Key Data Engineering Concepts Demonstrated

This project demonstrates practical experience with:

-   ETL / ELT pipelines
-   Event-driven data ingestion
-   Azure Data Factory
-   ADLS Gen2
-   Apache Spark
-   PySpark DataFrame API
-   Data type casting
-   Data cleansing
-   Null handling
-   Delta Lake
-   Delta MERGE / Upsert
-   Medallion Architecture
-   Unity Catalog
-   Managed Identity
-   External Locations
-   Databricks Jobs
-   Parameterized notebooks
-   Serverless Databricks compute
-   Databricks SQL
-   Data visualization
-   Git and GitHub
-   Cloud data engineering architecture

------------------------------------------------------------------------

## 🚀 How to Use This Repository

### Clone the repository

``` bash
git clone https://github.com/Shubhamoo/ecommerce-data-engineering.git
cd ecommerce-data-engineering
```

### Open the notebooks

Navigate to:

``` text
databricks/notebooks/
```

The notebooks can be imported into Azure Databricks.

> Note: The notebooks reference Azure resources such as ADLS Gen2, Unity
> Catalog, Delta tables, and Databricks compute. These resources must be
> configured in your own Azure environment before executing the
> notebooks.

------------------------------------------------------------------------

## 📌 Future Improvements

Possible improvements for the next version:

-   Add automated data quality checks.
-   Add unit tests for PySpark transformations.
-   Add CI/CD using GitHub Actions.
-   Add Azure DevOps or GitHub-based deployment.
-   Add incremental processing for all datasets.
-   Add monitoring and alerting.
-   Add data lineage documentation.
-   Add more Gold-layer analytical tables.
-   Add additional dashboard KPIs.
-   Add infrastructure-as-code using Terraform.
-   Add automated documentation of pipeline runs.

------------------------------------------------------------------------

## 👨‍💻 Author

**Shubham Kumar**

Software Engineer \| Data Engineering Enthusiast

### Skills Demonstrated

`Python` · `PySpark` · `SQL` · `Azure` · `Azure Data Factory` ·
`ADLS Gen2` · `Databricks` · `Delta Lake` · `Unity Catalog` · `Git` ·
`GitHub`

------------------------------------------------------------------------

## ⭐ Project Highlights

> An end-to-end Azure Data Engineering project demonstrating
> event-driven ingestion, distributed processing with PySpark, Delta
> Lake storage, Unity Catalog governance, SQL analytics, and business
> dashboards.
