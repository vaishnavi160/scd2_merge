# ETL Pipeline with Data Quality and SCD2 using Databricks & PySpark

This project implements a robust **data processing pipeline** using **Databricks**, **PySpark**, and **Delta Lake**, with built-in **data quality validation using Deequ** and **Slowly Changing Dimensions Type 2 (SCD2)** logic for maintaining historical customer data. It is designed to run daily via a **Databricks Workflow**, parameterized by arrival date.

---

## 📁 Project Components

- **Data Sources:**
  - Booking Data (CSV)
  - Customer Data (CSV)
- **Technologies Used:**
  - PySpark (on Databricks)
  - Delta Lake (for table management)
  - [PyDeequ](https://github.com/awslabs/python-deequ) (for data validation)
  - Unity Catalog / External Volumes
  - Databricks Workflows (for scheduling and automation)

---

## ⚙️ Workflow Overview

1. **Parameter-driven ingestion** using `arrival_date` widget.
2. **Reads daily partitioned data** from external volumes.
3. Runs **data quality checks** on both booking and customer data.
4. Performs **business transformations** on the booking data (e.g., total cost computation).
5. Aggregates booking data to load into a **Fact Table**.
6. Merges customer data using **SCD Type 2 logic** to update the **Dimension Table**.
7. All tables are stored in Delta format using Unity Catalog.

---

## 🔍 Key Features

- ✅ **Data Validation with Deequ**
  - Completeness, uniqueness, and non-negativity checks.
  - Validation fails the pipeline early if checks fail.
  
- 💾 **Fact Table: `booking_fact`**
  - Aggregates total cost and quantity per booking type and customer.

- 🕰 **SCD2 Dimension Table: `customer_dim`**
  - Tracks historical changes in customer data using `valid_to` and `valid_from`.

- 🔄 **Workflow Integration**
  - Designed to run as a daily scheduled job through **Databricks Workflows**.
  - Uses `dbutils.widgets.get("arrival_date")` to ingest data for a specific date.

---

## 🧪 Sample Data Flow

```plaintext
Raw CSVs → PySpark Load → Deequ Validation → Transformation & Aggregation
     → booking_fact (Delta Table)
     → customer_dim (Delta Table with SCD2 merge)
```
---

## 📌 Notes

- Make sure the `pydeequ` library is available on the cluster.  
  You can build it from source if needed: [PyDeequ GitHub](https://github.com/awslabs/python-deequ)

- Ensure **Unity Catalog** and **external volume access** are properly configured with the correct permissions.

- This pipeline assumes the presence of `valid_from` and `valid_to` columns in the customer dataset  
  for implementing **Slowly Changing Dimension Type 2 (SCD2)** logic.
