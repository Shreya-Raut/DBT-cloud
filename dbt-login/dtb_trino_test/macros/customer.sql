{% macro customer() %}
SELECT
    DATE_FORMAT(cal.report_week_commencing, '%Y-%m-%d') AS year_month_day,
    'week' AS date_scope,
    -- region,
    -- pl_flag AS pnl,
    CASE
        WHEN pl_flag = 'EU' AND region = 'United Kingdom' THEN 'UK>EU'
        WHEN pl_flag = 'EU' AND region = 'Unknown' THEN 'France'
        WHEN pl_flag = 'UK' THEN 'United Kingdom'
        ELSE region
    END AS region,

    'active_customers' AS metric_name,
    'Active Customers' AS metric_label,
    SUM(customers) AS metric_value
FROM
    {{ source('tableau', 'customer_growth_accounting') }} cga
    JOIN {{ source('calender', 'bi_d_calendar') }} cal
        ON cga."week" = cal.report_date
WHERE
    metric_label = 'Customer Base'
    AND SPLIT(managed_group_id, ' ')[1] IN ('20', '42')
    AND person_status IN ('New', 'Resurrected', 'Retained')
    {% if is_incremental()  %}
    AND DATE(cal.report_week_commencing) > (select max(year_month_day) from {{this}})
    {% endif %}-- active_customers
    -- AND person_status IN ('New') -- new_customers
    -- AND person_status IN ('Resurrected') -- resurrected_customers
    -- AND person_status IN ('Retained') -- retained_customers
    -- AND person_status IN ('Inactive') -- inactive_customers
    -- AND person_status IN ('Churned') -- churned_customers

GROUP BY 1,2,3,4
{% endmacro %}