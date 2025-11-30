-- Create schemas
CREATE DATABASE IF NOT EXISTS company_activity;
USE company_activity;

-- Staging tables
CREATE TABLE IF NOT EXISTS stg_crm_daily (
    company_id VARCHAR(50),
    name VARCHAR(255),
    country VARCHAR(100),
    industry_tag VARCHAR(100),
    last_contact_at DATETIME,
    load_date DATE
);

CREATE TABLE IF NOT EXISTS stg_product_usage_daily (
    company_id VARCHAR(50),
    date DATE,
    active_users INT,
    events INT
);

-- Analytics table
CREATE TABLE IF NOT EXISTS analytics_company_daily_activity (
    activity_date DATE,
    company_id VARCHAR(50),
    company_name VARCHAR(255),
    country VARCHAR(100),
    industry_tag VARCHAR(100),
    last_contact_at DATETIME,
    active_users INT,
    events INT,
    active_users_7d INT,
    is_churn_risk BOOLEAN,
    record_loaded_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (company_id, activity_date)
);
