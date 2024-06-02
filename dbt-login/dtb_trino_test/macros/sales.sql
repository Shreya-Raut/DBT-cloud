{% macro sales() %}
SELECT
    -- DATE(cal.report_date) AS report_date,
    DATE_FORMAT(cal.report_week_commencing, '%Y-%m-%d') AS year_month_day,
    'week' AS date_scope,
    CASE
        WHEN product_pl_code = 'EU' AND order_region_name = 'United Kingdom' THEN 'UK>EU'
        WHEN product_pl_code = 'EU' AND order_region_name = 'Unknown' THEN 'France'
        WHEN product_pl_code = 'UK' THEN 'United Kingdom'
        ELSE order_region_name
    END AS region,
    -- order_region_name AS region,
    -- product_pl_code AS pnl,

    'gross_sales_eur' AS metric_name,
    'Gross Sales' AS metric_label,
    CAST(SUM(m_gross_sales_amount_eur) AS DECIMAL(21,2)) AS metric_value

    -- 'gross_sales_gbp' AS metric_name,
    -- 'Gross Sales' AS metric_label,
    -- SUM(m_gross_sales_amount_gbp) AS metric_value

    -- 'net_sales_eur' AS metric_name,
    -- 'Net Sales' AS metric_label,
    -- SUM(m_net_sales_amount_eur) AS metric_value

    -- 'net_sales_gbp' AS metric_name,
    -- 'Net Sales' AS metric_label,
    -- SUM(m_net_sales_amount_gbp) AS metric_value

    -- 'revenue_eur' AS metric_name,
    -- 'Revenue' AS metric_label,
    -- SUM(
    --     m_revenue_product_commission_amount_eur
    --     + m_revenue_promo_deduction_amount_eur
    --     + m_revenue_booking_fee_eur
    --     + m_revenue_fx_margin_eur
    --     + m_revenue_insurance_commission_amount_eur
    --     + m_revenue_payment_fee_eur
    --     + m_revenue_webloyalty_allocation_amount_eur
    -- ) AS metric_value

    -- 'revenue_gbp' AS metric_name,
    -- SUM(
    --     m_revenue_product_commission_amount_gbp
    --     + m_revenue_promo_deduction_amount_gbp
    --     + m_revenue_booking_fee_gbp
    --     + m_revenue_fx_margin_gbp
    --     + m_revenue_insurance_commission_amount_gbp
    --     + m_revenue_payment_fee_gbp
    --     + m_revenue_webloyalty_allocation_amount_gbp
    -- ) AS metric_value

    -- 'costs_eur' AS metric_name,
    -- (
    --     COUNT(DISTINCT CASE WHEN product_pl_code = 'EU' AND order_region_name = 'France' THEN order_id END) * DECIMAL '0.4'
    --     + COUNT(DISTINCT CASE WHEN product_pl_code = 'EU' AND order_region_name = 'Germany' THEN order_id END) * DECIMAL '1.1'
    --     + COUNT(DISTINCT CASE WHEN product_pl_code = 'EU' AND order_region_name = 'Italy' THEN order_id END) * DECIMAL '0.3'
    --     + COUNT(DISTINCT CASE WHEN product_pl_code = 'EU' AND order_region_name = 'Spain' THEN order_id END) * DECIMAL '0.4'
    --     + COUNT(DISTINCT CASE WHEN product_pl_code = 'EU' AND order_region_name = 'RoE' THEN order_id END) * DECIMAL '0.9'
    --     + COUNT(DISTINCT CASE WHEN product_pl_code = 'EU' AND order_region_name = 'United Kingdom' THEN order_id END) * DECIMAL '0.7'
    --     + COUNT(DISTINCT CASE WHEN product_pl_code = 'EU' AND order_region_name = 'Inbound' THEN order_id END) * DECIMAL '4.8'
    --     + COUNT(DISTINCT CASE WHEN product_pl_code = 'UK' THEN order_id END) * DECIMAL '0.2'
    -- )
    -- + SUM(CASE WHEN (product_vendor_api_name IN ('SNCF PAO', 'OUIGO')) THEN m_costs_pao_fee_eur ELSE 0 END) AS metric_value


    -- 'costs_gbp' AS metric_name,
    -- (
    --     COUNT(DISTINCT CASE WHEN product_pl_code = 'EU' AND order_region_name = 'France' THEN order_id END) * DECIMAL '0.4'
    --     + COUNT(DISTINCT CASE WHEN product_pl_code = 'EU' AND order_region_name = 'Germany' THEN order_id END) * DECIMAL '1.1'
    --     + COUNT(DISTINCT CASE WHEN product_pl_code = 'EU' AND order_region_name = 'Italy' THEN order_id END) * DECIMAL '0.3'
    --     + COUNT(DISTINCT CASE WHEN product_pl_code = 'EU' AND order_region_name = 'Spain' THEN order_id END) * DECIMAL '0.4'
    --     + COUNT(DISTINCT CASE WHEN product_pl_code = 'EU' AND order_region_name = 'RoE' THEN order_id END) * DECIMAL '0.9'
    --     + COUNT(DISTINCT CASE WHEN product_pl_code = 'EU' AND order_region_name = 'United Kingdom' THEN order_id END) * DECIMAL '0.7'
    --     + COUNT(DISTINCT CASE WHEN product_pl_code = 'EU' AND order_region_name = 'Inbound' THEN order_id END) * DECIMAL '4.8'
    --     + COUNT(DISTINCT CASE WHEN product_pl_code = 'UK' THEN order_id END) * DECIMAL '0.2'
    -- )
    -- + SUM(CASE WHEN (product_vendor_api_name IN ('SNCF PAO', 'OUIGO')) THEN m_costs_pao_fee_gbp ELSE 0 END) AS metric_value

FROM
    {{ source('bi_dwh', 'fm_products') }} f
    LEFT JOIN {{ source('calender', 'bi_d_calendar') }} cal
        ON DATE(f.year_month_day) = DATE(cal.report_date)
WHERE
    f.order_business_channel_code = 'Leisure'
    -- AND DATE(cal.report_week_commencing) = DATE('2024-03-31')
    -- AND DATE(f.year_month_day) >= DATE('2024-03-31')

GROUP BY 1,2,3
{% endmacro %}
