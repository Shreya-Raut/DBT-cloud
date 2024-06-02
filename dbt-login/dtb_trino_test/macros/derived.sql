{% macro derived() %}
SELECT
    year_month_day,
    date_scope,
    region,

    --Gross Margin EUR
    'gross_margin_eur' AS metric_name,
    'Gross Margin' AS metric_label,
    ARBITRARY(metric_value) FILTER (WHERE metric_name = 'revenue_eur') - ARBITRARY(metric_value) FILTER (WHERE metric_name = 'costs_eur') AS metric_value

    --Gross Margin GBP
    -- 'gross_margin_gbp' AS metric_name,
    -- ARBITRARY(metric_value) FILTER (WHERE metric_name = 'revenue_gbp') - ARBITRARY(metric_value) FILTER (WHERE metric_name = 'costs_gbp') AS metric_value

    --Gross Margin - PMS
    --Does this need to be pre-calculated?

    --ATV EUR
    -- 'atv_eur' AS metric_name,
    -- ARBITRARY(metric_value) FILTER (WHERE metric_name = 'marketing_sales_eur') * 1.0 / ARBITRARY(metric_value) FILTER (WHERE metric_name = 'transactions') AS metric_value

    --ATV GBP
    -- 'atv_gbp' AS metric_name,
    -- ARBITRARY(metric_value) FILTER (WHERE metric_name = 'marketing_sales_gbp') * 1.0 / ARBITRARY(metric_value) FILTER (WHERE metric_name = 'transactions') AS metric_value

    --Conversion
    -- 'conversion_rate' AS metric_name,
    -- ARBITRARY(metric_value) FILTER (WHERE metric_name = 'transactions_tracked') * 1.0 / ARBITRARY(metric_value) FILTER (WHERE metric_name = 'visits') AS metric_value

FROM
    {{ source('reporting', 'business_health_demo') }}
GROUP BY 1,2,3

{% endmacro %}