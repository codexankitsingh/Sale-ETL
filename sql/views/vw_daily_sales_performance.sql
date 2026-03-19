-- ═══════════════════════════════════════════════════════════
-- View : vw_daily_sales_performance
-- Dataset : {analytics_dataset}
-- Purpose : Daily revenue and order KPIs aggregated by date
--           Used for trend charts in Looker Studio
-- Tables  : orders + order_payments + order_items
-- ═══════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW `{project}.{analytics_dataset}.vw_daily_sales_performance` AS

WITH

-- ── Payment aggregates per order ──────────────────────────
payment_agg AS (
    SELECT
        order_id,
        ROUND(SUM(payment_value), 2)            AS total_payment_value
    FROM `{project}.{raw_dataset}.order_payments`
    GROUP BY order_id
),

-- ── Item aggregates per order ─────────────────────────────
item_agg AS (
    SELECT
        order_id,
        COUNT(order_item_id)                    AS total_items,
        ROUND(SUM(total_item_value), 2)         AS order_gmv,
        ROUND(SUM(freight_value), 2)            AS freight_total
    FROM `{project}.{raw_dataset}.order_items`
    GROUP BY order_id
),

-- ── Orders enriched with payment + item data ──────────────
orders_enriched AS (
    SELECT
        o.order_id,
        o.sale_date,
        EXTRACT(YEAR  FROM o.sale_date)         AS sale_year,
        EXTRACT(MONTH FROM o.sale_date)         AS sale_month,
        FORMAT_DATE('%A', o.sale_date)          AS sale_day_name,
        FORMAT_DATE('%Y-%m', o.sale_date)       AS sale_yearmonth,

        -- Delivery flags
        o.order_status = 'delivered'            AS is_delivered,
        o.order_status = 'canceled'             AS is_canceled,

        -- Delivery delay
        CASE
            WHEN o.order_delivered_customer_date IS NOT NULL
             AND o.order_estimated_delivery_date IS NOT NULL
            THEN DATE_DIFF(
                DATE(o.order_delivered_customer_date),
                DATE(o.order_estimated_delivery_date),
                DAY
            )
            ELSE NULL
        END                                     AS delivery_delay_days,

        -- Shipping days
        CASE
            WHEN o.order_delivered_customer_date IS NOT NULL
             AND o.order_delivered_carrier_date  IS NOT NULL
            THEN DATE_DIFF(
                DATE(o.order_delivered_customer_date),
                DATE(o.order_delivered_carrier_date),
                DAY
            )
            ELSE NULL
        END                                     AS shipping_days,

        p.total_payment_value,
        i.order_gmv,
        i.freight_total,
        i.total_items

    FROM `{project}.{raw_dataset}.orders` o
    LEFT JOIN payment_agg p ON o.order_id = p.order_id
    LEFT JOIN item_agg    i ON o.order_id = i.order_id
)

-- ── Daily aggregation ─────────────────────────────────────
SELECT
    sale_date,
    sale_year,
    sale_month,
    sale_day_name,
    sale_yearmonth,

    -- Order counts
    COUNT(DISTINCT order_id)                    AS total_orders,
    COUNTIF(is_delivered)                       AS delivered_orders,
    COUNTIF(is_canceled)                        AS canceled_orders,
    COUNT(DISTINCT order_id)
        - COUNTIF(is_delivered)
        - COUNTIF(is_canceled)                  AS pending_orders,

    -- Revenue metrics
    ROUND(SUM(total_payment_value), 2)          AS daily_revenue,
    ROUND(AVG(total_payment_value), 2)          AS avg_order_value,
    ROUND(SUM(order_gmv), 2)                    AS daily_gmv,
    ROUND(SUM(freight_total), 2)                AS daily_freight,
    ROUND(SUM(total_items), 0)                  AS total_items_sold,

    -- Delivery metrics
    ROUND(AVG(delivery_delay_days), 2)          AS avg_delivery_delay_days,
    ROUND(AVG(shipping_days), 2)                AS avg_shipping_days,

    -- Cancellation rate
    ROUND(
        SAFE_DIVIDE(
            COUNTIF(is_canceled),
            COUNT(DISTINCT order_id)
        ) * 100, 2
    )                                           AS cancellation_rate_pct

FROM orders_enriched
GROUP BY
    sale_date,
    sale_year,
    sale_month,
    sale_day_name,
    sale_yearmonth
ORDER BY sale_date
