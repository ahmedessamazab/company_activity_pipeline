USE company_activity;

INSERT INTO analytics_company_daily_activity
SELECT
    u.date AS activity_date,
    u.company_id,
    c.name AS company_name,
    c.country,
    c.industry_tag,
    c.last_contact_at,
    u.active_users,
    u.events,

    -- Rolling 7-day active users
    (
        SELECT SUM(u2.active_users)
        FROM stg_product_usage_daily u2
        WHERE u2.company_id = u.company_id
          AND u2.date BETWEEN DATE_SUB(u.date, INTERVAL 6 DAY) AND u.date
    ) AS active_users_7d,

    -- Churn risk: no active users for 7 consecutive days
    CASE
        WHEN (
            SELECT SUM(u3.active_users)
            FROM stg_product_usage_daily u3
            WHERE u3.company_id = u.company_id
              AND u3.date BETWEEN DATE_SUB(u.date, INTERVAL 6 DAY) AND u.date
        ) = 0 THEN 1
        ELSE 0
    END AS is_churn_risk,

    NOW() AS record_loaded_at

FROM stg_product_usage_daily u
LEFT JOIN stg_crm_daily c
ON u.company_id = c.company_id;
