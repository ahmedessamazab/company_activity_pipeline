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


| -------------------------------------------------------------------------- |
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
| -------------------------------------------------------------------------- |


## 2. Sample SQL

The full transformation SQL is located here:

➡️ **sql/populate_company_daily_activity.sql**

A staging table DDL example is here:

➡️ **sql/create_tables.sql**

## 3. ADF Flow
...

## 4. API Ingestion Pseudocode
...

## 5. 30-Minute Constraint Reasoning
...
