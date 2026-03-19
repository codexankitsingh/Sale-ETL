-- ═══════════════════════════════════════════════════════════
-- View : vw_order_summary
-- Dataset : {analytics_dataset}
-- Purpose : Master order view with derived delivery metrics,
--           payment aggregates and item aggregates per order
-- Tables  : orders + order_items + order_payments
-- ═══════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW `{project}.{analytics_dataset}.vw_order_summary` AS

WITH

-- ── Payment aggregates per order ──────────────────────────
payment_agg AS (
    SELECT
        order_id,
        ROUND(SUM(payment_value), 2)            AS total_payment_value,
        MAX(payment_installments)               AS max_installments,
        -- Primary payment type = type with highest value
        ARRAY_AGG(
            payment_type
            ORDER BY payment_value DESC
            LIMIT 1
        )[OFFSET(0)]                            AS primary_payment_type,
        COUNT(payment_sequential)               AS payment_parts_count
    FROM `{project}.{raw_dataset}.order_payments`
    GROUP BY order_id
),

-- ── Item aggregates per order ─────────────────────────────
item_agg AS (
    SELECT
        order_id,
        COUNT(order_item_id)                    AS total_items,
        COUNT(DISTINCT product_id)              AS unique_products,
        COUNT(DISTINCT seller_id)               AS unique_sellers,
        ROUND(SUM(price), 2)                    AS items_subtotal,
        ROUND(SUM(freight_value), 2)            AS freight_total,
        ROUND(SUM(total_item_value), 2)         AS order_gmv
    FROM `{project}.{raw_dataset}.order_items`
    GROUP BY order_id
)

-- ── Main SELECT ───────────────────────────────────────────
SELECT
    -- Order identifiers
    o.order_id,
    o.customer_trx_id,
    o.order_status,

    -- Timestamps
    o.order_purchase_timestamp,
    o.order_approved_at,
    o.order_delivered_carrier_date,
    o.order_delivered_customer_date,
    o.order_estimated_delivery_date,

    -- Date dimensions derived from purchase timestamp
    o.sale_date,
    EXTRACT(YEAR  FROM o.sale_date)             AS sale_year,
    EXTRACT(MONTH FROM o.sale_date)             AS sale_month,
    EXTRACT(DAY   FROM o.sale_date)             AS sale_day,
    FORMAT_DATE('%A', o.sale_date)              AS sale_day_name,
    FORMAT_DATE('%Y-%m', o.sale_date)           AS sale_yearmonth,

    -- Delivery status flags
    CASE
        WHEN o.order_status = 'delivered'
        THEN TRUE ELSE FALSE
    END                                         AS is_delivered,

    CASE
        WHEN o.order_status = 'canceled'
        THEN TRUE ELSE FALSE
    END                                         AS is_canceled,

    CASE
        WHEN o.order_status IN ('shipped', 'out_for_delivery')
        THEN TRUE ELSE FALSE
    END                                         AS is_in_transit,

    -- Delivery delay in days (positive = late, negative = early)
    CASE
        WHEN o.order_delivered_customer_date IS NOT NULL
         AND o.order_estimated_delivery_date IS NOT NULL
        THEN DATE_DIFF(
            DATE(o.order_delivered_customer_date),
            DATE(o.order_estimated_delivery_date),
            DAY
        )
        ELSE NULL
    END                                         AS delivery_delay_days,

    -- Shipping days (carrier pickup → customer delivery)
    CASE
        WHEN o.order_delivered_customer_date IS NOT NULL
         AND o.order_delivered_carrier_date IS NOT NULL
        THEN DATE_DIFF(
            DATE(o.order_delivered_customer_date),
            DATE(o.order_delivered_carrier_date),
            DAY
        )
        ELSE NULL
    END                                         AS shipping_days,

    -- Order cycle days (purchase → delivery)
    CASE
        WHEN o.order_delivered_customer_date IS NOT NULL
        THEN DATE_DIFF(
            DATE(o.order_delivered_customer_date),
            DATE(o.order_purchase_timestamp),
            DAY
        )
        ELSE NULL
    END                                         AS order_cycle_days,

    -- Payment aggregates
    p.total_payment_value,
    p.max_installments,
    p.primary_payment_type,
    p.payment_parts_count,

    -- Item aggregates
    i.total_items,
    i.unique_products,
    i.unique_sellers,
    i.items_subtotal,
    i.freight_total,
    i.order_gmv,

    -- Data quality flag
    ROUND(
        ABS(
            COALESCE(p.total_payment_value, 0) -
            COALESCE(i.order_gmv, 0)
        ), 2
    )                                           AS payment_vs_gmv_diff,

    o.etl_timestamp

FROM `{project}.{raw_dataset}.orders` o
LEFT JOIN payment_agg p ON o.order_id = p.order_id
LEFT JOIN item_agg    i ON o.order_id = i.order_id
