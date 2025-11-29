CREATE SCHEMA IF NOT EXISTS analytics;
CREATE SCHEMA IF NOT EXISTS stg;

CREATE TABLE IF NOT EXISTS stg.crm_daily (
    company_id VARCHAR(128),
    name VARCHAR(255),
    country VARCHAR(128),
    industry_tag VARCHAR(128),
    last_contact_at DATETIME,
    load_date DATE
);

CREATE TABLE IF NOT EXISTS stg.product_usage_daily (
    company_id VARCHAR(128),
    date DATE,
    active_users INT,
    events INT
);

CREATE TABLE IF NOT EXISTS analytics.company_daily_activity (
    activity_date DATE,
    company_id VARCHAR(128),
    company_name VARCHAR(255),
    country VARCHAR(128),
    industry_tag VARCHAR(128),
    last_contact_at DATETIME,
    active_users INT,
    events INT,
    active_users_7d INT,
    is_churn_risk BOOLEAN,
    record_loaded_at DATETIME,
    PRIMARY KEY(company_id, activity_date)
);
