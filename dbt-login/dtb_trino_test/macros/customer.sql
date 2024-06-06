{% macro customer() %}
    select
        date_format(cal.report_week_commencing, '%Y-%m-%d') as year_month_day,
        'week' as date_scope,
        -- region,
        -- pl_flag AS pnl,
        case
            when pl_flag = 'EU' and region = 'United Kingdom'
            then 'UK>EU'
            when pl_flag = 'EU' and region = 'Unknown'
            then 'France'
            when pl_flag = 'UK'
            then 'United Kingdom'
            else region
        end as region,

<<<<<<< HEAD
        'active_customers' as metric_name,
        'Active Customers' as metric_label,
        sum(customers) as metric_value
    from {{ source("tableau", "customer_growth_accounting") }} cga
    join {{ source("calender", "bi_d_calendar") }} cal on cga."week" = cal.report_date
    where
        metric_label = 'Customer Base'
        and split(managed_group_id, ' ')[1] in ('20', '42')
        and person_status in ('New', 'Resurrected', 'Retained')
        {% if is_incremental() %}
            AND DATE(cal.report_week_commencing) = DATE('2024-05-26')
        {% endif %}  -- active_customers
=======
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
    AND DATE(cal.report_week_commencing) > DATE((select max(year_month_day) from {{this}}))
    {% endif %}-- active_customers
>>>>>>> eff0757024226fae5bd48518ee0b066e5b9f6ce0
    -- AND person_status IN ('New') -- new_customers
    -- AND person_status IN ('Resurrected') -- resurrected_customers
    -- AND person_status IN ('Retained') -- retained_customers
    -- AND person_status IN ('Inactive') -- inactive_customers
    -- AND person_status IN ('Churned') -- churned_customers
    group by 1, 2, 3, 4
{% endmacro %}
