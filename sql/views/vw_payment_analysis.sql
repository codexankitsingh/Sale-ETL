-- ═══════════════════════════════════════════════════════════
-- View : vw_payment_analysis
-- Dataset : {analytics_dataset}
-- Purpose : Payment method trends, installment behaviour
--           and monthly revenue share per payment type
-- Tables  : order_payments + orders
-- ═══════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW `{project}.{analytics_dataset}.vw_payment_analysis` AS

WITH

-- ── Order date dimensions ─────────────────────────────────
order_dates AS (
    SELECT
        order_id,
        sale_date,
        EXTRACT(YEAR  FROM sale_date)           AS sale_year,
        EXTRACT(MONTH FROM sale_date)           AS sale_month,
        FORMAT_DATE('%Y-%m', sale_date)         AS sale_yearmonth,
        order_status = 'delivered'              AS is_delivered,
        order_status = 'canceled'               AS is_canceled
    FROM `{project}.{raw_dataset}.orders`
),

-- ── Payments joined with order dates ─────────────────────
payments_enriched AS (
    SELECT
        op.order_id,
        op.payment_sequential,
        op.payment_type,
        op.payment_installments,
        op.payment_value,
        od.sale_date,
        od.sale_year,
        od.sale_month,
        od.sale_yearmonth,
        od.is_delivered,
        od.is_canceled
    FROM `{project}.{raw_dataset}.order_payments` op
    LEFT JOIN order_dates od ON op.order_id = od.order_id
),

-- ── Monthly totals for window calculation ─────────────────
monthly_totals AS (
    SELECT
        sale_yearmonth,
        ROUND(SUM(payment_value), 2)            AS monthly_total_revenue
    FROM payments_enriched
    GROUP BY sale_yearmonth
)

-- ── Final aggregation by payment type + month ─────────────
SELECT
    pe.payment_type,
    pe.sale_year,
    pe.sale_month,
    pe.sale_yearmonth,

    -- Volume metrics
    COUNT(DISTINCT pe.order_id)                 AS total_orders,
    COUNT(pe.payment_sequential)                AS total_payment_parts,

    -- Revenue metrics
    ROUND(SUM(pe.payment_value), 2)             AS total_payment_value,
    ROUND(AVG(pe.payment_value), 2)             AS avg_payment_value,
    ROUND(MIN(pe.payment_value), 2)             AS min_payment_value,
    ROUND(MAX(pe.payment_value), 2)             AS max_payment_value,

    -- Installment metrics
    ROUND(AVG(pe.payment_installments), 2)      AS avg_installments,
    MAX(pe.payment_installments)                AS max_installments,
    COUNTIF(pe.payment_installments > 1)        AS installment_orders,
    COUNTIF(pe.payment_installments = 1)        AS single_payment_orders,

    -- Delivery context
    COUNTIF(pe.is_delivered)                    AS delivered_orders,
    COUNTIF(pe.is_canceled)                     AS canceled_orders,

    -- Share of monthly revenue
    ROUND(
        SAFE_DIVIDE(
            SUM(pe.payment_value),
            mt.monthly_total_revenue
        ) * 100, 2
    )                                           AS pct_of_monthly_revenue,

    -- Installment usage rate
    ROUND(
        SAFE_DIVIDE(
            COUNTIF(pe.payment_installments > 1),
            COUNT(DISTINCT pe.order_id)
        ) * 100, 2
    )                                           AS installment_usage_pct

FROM payments_enriched pe
LEFT JOIN monthly_totals mt
    ON pe.sale_yearmonth = mt.sale_yearmonth
GROUP BY
    pe.payment_type,
    pe.sale_year,
    pe.sale_month,
    pe.sale_yearmonth,
    mt.monthly_total_revenue
ORDER BY
    pe.sale_yearmonth,
    total_payment_value DESC
