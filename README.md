# Company Activity Pipeline & Data Model

## 1. Target Analytics Table Design
Grain: One row per company per day

This is the lowest meaningful grain because:

1- The product-usage API only returns daily aggregates

2- The CRM CSV is also a daily snapshot

3- No event-level or hourly data exists

Daily grain supports these analytics:

    - daily active users
    - 7-day rolling metrics
    - churn detection
    - weekly/monthly trends

Table: analytics.company_daily_activity


| Column           | Type      | Source  | Description                       |
| ---------------- | --------- | ------- | --------------------------------- |
| activity_date    | DATE      | API     | Date of activity                  |
| company_id       | STRING    | CRM/API | Unique company identifier         |
| company_name     | STRING    | CRM     | Latest company name               |
| country          | STRING    | CRM     | Country                           |
| industry_tag     | STRING    | CRM     | Industry classification           |
| last_contact_at  | DATETIME  | CRM     | Last CRM contact timestamp        |
| active_users     | INT       | API     | Daily active users count          |
| events           | INT       | API     | Total events for that day         |
| 7d_active_users  | INT       | Derived | Rolling 7-day sum of active users |
| is_churn_risk    | BOOLEAN   | Derived | 1 if no usage for ≥7 days         |
| record_loaded_at | TIMESTAMP | ETL     | Load timestamp                    |


## 2. Sample SQL

The full transformation SQL is located here:

➡️ **sql/populate_company_daily_activity.sql**

A staging table DDL example is here:

➡️ **sql/create_tables.sql**

## 3. Azure Data Factory (ADF) Flow

The pipeline follows a simple and reliable orchestration pattern with three main pipelines and one master pipeline.

---

### **📌 Pipeline 1 — `pl_ingest_crm_daily`**
**Source:** Daily CRM CSV stored in Azure Blob  
**Steps:**
- Copy Activity → load CSV into `stg.crm_daily`
- Validate row count > 0 (Data Flow or Stored Procedure validation)
- OnFailure → Azure Monitor alert → Teams/Email

---

### **📌 Pipeline 2 — `pl_ingest_product_usage`**
**Source:** Product Usage REST API  
**Steps:**
- Web Activity → call product API for the given date
- Sink raw JSON/CSV to Blob (`raw/product_usage/`)
- Copy Activity → load into `stg.product_usage_daily`
- OnFailure → Alert

The API ingestion is the most critical part due to:
- authentication risks  
- rate limits  
- potential for missing daily data  

---

### **📌 Pipeline 3 — `pl_build_company_daily_activity`**
**Steps:**
- Execute Stored Procedure or SQL Script  
  - runs `sql/populate_company_daily_activity.sql`
  - merges results into `analytics.company_daily_activity`
- OnFailure → Alert

---

### **📌 Master Pipeline — `pl_master_company_activity`**
**Schedule:** Daily at 00:00 UTC

**Order of execution:**
1. `pl_ingest_crm_daily`  
2. `pl_ingest_product_usage`  
3. `pl_build_company_daily_activity`  

---

### **📌 Monitoring & Alerting**
- Azure Monitor alerts on:
  - pipeline failure  
  - activity-level failure  
  - >10% drop in daily record count  (if we have information and more knowlege about it)
- Alerts routed to:
  - Microsoft Teams  
  - Email

## 4. API Ingestion Pseudocode

The pseudocode is located here:

➡️ api/fetch_product_usage_pseudocode.py


## 5. 30-Minute Constraint Reasoning

If I only had **30 minutes before tomorrow’s scheduled pipeline run**, I would implement the part of the pipeline that carries the **highest risk** and would cause the **largest impact** if missing.

### I would implement first: `pl_ingest_product_usage` (API ingestion)

**Reasoning:**

- The product-usage API is the **most fragile** part of the pipeline  
- API failures can occur due to:
  - authentication issues  
  - rate limits  
  - schema changes  
  - network/timeouts  
- If API ingestion fails, we lose that day's usage data permanently  
- Missing usage data breaks:
  - 7-day rolling metrics  
  - churn analysis  
  - daily/weekly trend charts  

In short:

> **No API data → the dashboard is useless.**

---

### What I would postpone (safe to delay)

These can run later without breaking tomorrow’s run:

- pl_ingest_crm_daily because it will be straitforward and alos because the data will be already in the blob and there will be missing data.
- pl_build_company_daily_activity because it mean nathing to create this without having the api data and loosing the api data for that day.
- Rolling metric calculations (`7d_active_users`)
- Churn detection (`is_churn_risk`)
- Documentation, diagrams, optimization

These are important but **not critical for tomorrow’s success**.

---

### Summary

> If I only had 30 minutes, I would ensure **API ingestion is working end-to-end**, because it is the highest-risk component and the only part that can permanently break the pipeline if not ready. 