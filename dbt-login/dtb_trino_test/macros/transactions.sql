{% macro transactions() %}
SELECT
    DATE_FORMAT(cal.report_week_commencing, '%Y-%m-%d') AS year_month_day,
    'week' AS date_scope,
    region,
    -- CASE pnl_segment
    --     WHEN 'INT' THEN 'EU'
    --     WHEN 'INTL' THEN 'EU'
    --     ELSE pnl_segment
    -- END AS pnl,

    'transactions' AS metric_name,
    'Transactions' AS metric_label,
    SUM(value) AS metric_value


FROM
    {{ source('bi_dwh', 'marketing_mart') }} m
    LEFT JOIN {{ source('calender', 'bi_d_calendar') }} cal
        ON DATE(cal.report_date) = DATE(m.activity_date)
WHERE
    DATE(cal.report_week_commencing) = DATE('2024-03-31')
    AND kpi IN ('Transactions') -- transactions
    -- AND kpi IN ('Sales EUR') -- marketing_sales_eur
    -- AND kpi IN ('Sales GBP') -- marketing_sales_gbp
    -- AND kpi IN ('Visits') -- visits
    -- AND kpi IN ('Spend EUR') -- marketing_spend_eur
    -- AND kpi IN ('Spend GBP') -- marketing_spend_gbp
    -- AND kpi IN ('Transactions Tracked') -- transactions_tracked


GROUP BY 1, 2, 3
{% endmacro %}