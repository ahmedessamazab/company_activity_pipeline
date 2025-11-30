INSERT INTO analytics.company_daily_activity (
    activity_date,
    company_id,
    company_name,
    country,
    industry_tag,
    last_contact_at,
    active_users,
    events,
    active_users_7d,
    is_churn_risk,
    record_loaded_at
)
SELECT 
    u.activity_date,
    u.company_id,
    c.name,
    c.country,
    c.industry_tag,
    c.last_contact_at,
    u.active_users,
    u.events,

    -- 7d rolling window
    SUM(u.active_users) OVER (
        PARTITION BY u.company_id
        ORDER BY u.activity_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS active_users_7d,

    CASE 
        WHEN u.active_users = 0 AND
             SUM(CASE WHEN u.active_users > 0 THEN 1 END) 
             OVER (PARTITION BY u.company_id 
                   ORDER BY u.activity_date
                   ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) = 0
        THEN 1 ELSE 0 
    END AS is_churn_risk,

    NOW()

FROM stg_product_usage_daily u
LEFT JOIN stg_crm_daily c 
    ON u.company_id = c.company_id;
