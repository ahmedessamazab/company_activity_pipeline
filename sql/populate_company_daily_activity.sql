WITH crm AS (
    SELECT 
        company_id,
        name AS company_name,
        country,
        industry_tag,
        last_contact_at,
        CAST(load_date AS DATE) AS crm_date
    FROM stg.crm_daily
),
usage_cte AS (
    SELECT
        company_id,
        CAST([date] AS DATE) AS activity_date,
        active_users,
        events
    FROM stg.product_usage_daily
),
joined AS (
    SELECT
        u.activity_date,
        u.company_id,
        c.company_name,
        c.country,
        c.industry_tag,
        c.last_contact_at,
        u.active_users,
        u.events
    FROM usage_cte u
    LEFT JOIN crm c
        ON u.company_id = c.company_id
)
SELECT
    *,
    -- Rolling 7-day active users
    SUM(active_users) OVER (
        PARTITION BY company_id
        ORDER BY activity_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS active_users_7d,

    -- Churn risk flag
    CASE 
        WHEN active_users = 0 AND 
             MAX(CASE WHEN active_users > 0 THEN activity_date END) 
                OVER (
                    PARTITION BY company_id 
                    ORDER BY activity_date
                    ROWS BETWEEN 7 PRECEDING AND CURRENT ROW
                ) IS NULL
        THEN 1 ELSE 0 
    END AS is_churn_risk,

    CURRENT_TIMESTAMP AS record_loaded_at
FROM joined;
