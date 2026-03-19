-- ═══════════════════════════════════════════════════════════
-- View : vw_customer_profile
-- Dataset : {analytics_dataset}
-- Purpose : Customer lifetime value, order history,
--           demographics and geo enrichment
-- Tables  : customers + orders + order_payments + geolocations
-- ═══════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW `{project}.{analytics_dataset}.vw_customer_profile` AS

WITH

-- ── Payment aggregates per order ──────────────────────────
payment_agg AS (
    SELECT
        order_id,
        ROUND(SUM(payment_value), 2)            AS total_payment_value
    FROM `{project}.{raw_dataset}.order_payments`
    GROUP BY order_id
),

-- ── Order + payment joined ────────────────────────────────
order_payment AS (
    SELECT
        o.order_id,
        o.customer_trx_id,
        o.sale_date,
        o.order_status = 'delivered'            AS is_delivered,
        o.order_status = 'canceled'             AS is_canceled,
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
        p.total_payment_value
    FROM `{project}.{raw_dataset}.orders` o
    LEFT JOIN payment_agg p ON o.order_id = p.order_id
),

-- ── Customer order aggregates ─────────────────────────────
customer_agg AS (
    SELECT
        customer_trx_id,
        COUNT(DISTINCT order_id)                AS total_orders,
        ROUND(SUM(total_payment_value), 2)      AS lifetime_value,
        ROUND(AVG(total_payment_value), 2)      AS avg_order_value,
        ROUND(MAX(total_payment_value), 2)      AS max_order_value,
        ROUND(MIN(total_payment_value), 2)      AS min_order_value,
        MIN(sale_date)                          AS first_order_date,
        MAX(sale_date)                          AS last_order_date,
        DATE_DIFF(
            MAX(sale_date),
            MIN(sale_date),
            DAY
        )                                       AS customer_tenure_days,
        COUNTIF(is_delivered)                   AS delivered_orders,
        COUNTIF(is_canceled)                    AS canceled_orders,
        ROUND(AVG(delivery_delay_days), 2)      AS avg_delivery_delay_days
    FROM order_payment
    GROUP BY customer_trx_id
),

-- ── Geo lookup (one row per postal code) ──────────────────
geo_lookup AS (
    SELECT
        geo_postal_code,
        AVG(geo_lat)                            AS geo_lat,
        AVG(geo_lon)                            AS geo_lon
    FROM `{project}.{raw_dataset}.geolocations`
    GROUP BY geo_postal_code
)

-- ── Final SELECT ──────────────────────────────────────────
SELECT
    -- Customer identifiers
    c.customer_trx_id,
    c.subscriber_id,

    -- Demographics
    c.age,
    c.gender,
    CASE
        WHEN c.age < 25              THEN 'Gen Z (< 25)'
        WHEN c.age BETWEEN 25 AND 34 THEN 'Millennial (25-34)'
        WHEN c.age BETWEEN 35 AND 44 THEN 'Gen X (35-44)'
        WHEN c.age BETWEEN 45 AND 54 THEN 'Boomer (45-54)'
        WHEN c.age >= 55             THEN 'Senior (55+)'
        ELSE                              'Unknown'
    END                                         AS age_group,

    -- Dates
    c.subscribe_date,
    c.first_order_date,

    -- Geography
    c.customer_postal_code,
    c.customer_city,
    c.customer_country,
    c.customer_country_code,
    g.geo_lat,
    g.geo_lon,

    -- Order metrics
    COALESCE(a.total_orders, 0)                 AS total_orders,
    COALESCE(a.lifetime_value, 0)               AS lifetime_value,
    COALESCE(a.avg_order_value, 0)              AS avg_order_value,
    COALESCE(a.max_order_value, 0)              AS max_order_value,
    COALESCE(a.min_order_value, 0)              AS min_order_value,
    a.first_order_date                          AS first_purchase_date,
    a.last_order_date                           AS last_purchase_date,
    COALESCE(a.customer_tenure_days, 0)         AS customer_tenure_days,
    COALESCE(a.delivered_orders, 0)             AS delivered_orders,
    COALESCE(a.canceled_orders, 0)              AS canceled_orders,
    COALESCE(a.avg_delivery_delay_days, 0)      AS avg_delivery_delay_days,

    -- Customer value segment
    CASE
        WHEN COALESCE(a.lifetime_value, 0) >= 1000 THEN 'High Value'
        WHEN COALESCE(a.lifetime_value, 0) >= 500  THEN 'Mid Value'
        WHEN COALESCE(a.lifetime_value, 0) >= 100  THEN 'Low Value'
        ELSE                                             'Inactive'
    END                                         AS customer_segment,

    c.etl_timestamp

FROM `{project}.{raw_dataset}.customers` c
LEFT JOIN customer_agg a
    ON  c.customer_trx_id = a.customer_trx_id
LEFT JOIN geo_lookup g
    ON  c.customer_postal_code = g.geo_postal_code
