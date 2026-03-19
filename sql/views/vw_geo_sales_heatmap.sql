-- ═══════════════════════════════════════════════════════════
-- View : vw_geo_sales_heatmap
-- Dataset : {analytics_dataset}
-- Purpose : City and country level revenue heatmap
--           with lat/lon for Looker Studio geo charts
-- Tables  : customers + orders + order_payments
--           + order_reviews + geolocations
-- ═══════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW `{project}.{analytics_dataset}.vw_geo_sales_heatmap` AS

WITH

-- ── Payment aggregates per order ──────────────────────────
payment_agg AS (
    SELECT
        order_id,
        ROUND(SUM(payment_value), 2)            AS total_payment_value
    FROM `{project}.{raw_dataset}.order_payments`
    GROUP BY order_id
),

-- ── Review aggregates per order ───────────────────────────
review_agg AS (
    SELECT
        order_id,
        ROUND(AVG(review_score), 2)             AS avg_review_score
    FROM `{project}.{raw_dataset}.order_reviews`
    GROUP BY order_id
),

-- ── Orders enriched ───────────────────────────────────────
order_enriched AS (
    SELECT
        o.order_id,
        o.customer_trx_id,
        o.sale_date,
        EXTRACT(YEAR  FROM o.sale_date)         AS sale_year,
        EXTRACT(MONTH FROM o.sale_date)         AS sale_month,
        FORMAT_DATE('%Y-%m', o.sale_date)       AS sale_yearmonth,
        o.order_status = 'delivered'            AS is_delivered,
        o.order_status = 'canceled'             AS is_canceled,
        p.total_payment_value,
        r.avg_review_score
    FROM `{project}.{raw_dataset}.orders` o
    LEFT JOIN payment_agg p ON o.order_id = p.order_id
    LEFT JOIN review_agg  r ON o.order_id = r.order_id
),

-- ── Geo lookup — one coord per postal code ────────────────
geo_lookup AS (
    SELECT
        geo_postal_code,
        geolocation_city,
        geo_country,
        AVG(geo_lat)                            AS geo_lat,
        AVG(geo_lon)                            AS geo_lon
    FROM `{project}.{raw_dataset}.geolocations`
    GROUP BY
        geo_postal_code,
        geolocation_city,
        geo_country
),

-- ── Customer + order + geo joined ─────────────────────────
customer_orders AS (
    SELECT
        c.customer_trx_id,
        c.customer_postal_code,
        c.customer_city,
        c.customer_country,
        c.customer_country_code,
        g.geo_lat,
        g.geo_lon,
        g.geolocation_city                      AS geo_city,
        oe.order_id,
        oe.sale_date,
        oe.sale_year,
        oe.sale_month,
        oe.sale_yearmonth,
        oe.is_delivered,
        oe.is_canceled,
        oe.total_payment_value,
        oe.avg_review_score
    FROM `{project}.{raw_dataset}.customers` c
    LEFT JOIN order_enriched oe
        ON  c.customer_trx_id = oe.customer_trx_id
    LEFT JOIN geo_lookup g
        ON  c.customer_postal_code = g.geo_postal_code
)

-- ── Final aggregation by city + country ───────────────────
SELECT
    -- Location dimensions
    customer_country,
    customer_country_code,
    customer_city,
    customer_postal_code,
    geo_lat,
    geo_lon,

    -- Time dimensions
    sale_year,
    sale_month,
    sale_yearmonth,

    -- Order metrics
    COUNT(DISTINCT order_id)                    AS total_orders,
    COUNT(DISTINCT customer_trx_id)             AS unique_customers,
    COUNTIF(is_delivered)                       AS delivered_orders,
    COUNTIF(is_canceled)                        AS canceled_orders,

    -- Revenue metrics
    ROUND(SUM(total_payment_value), 2)          AS total_revenue,
    ROUND(AVG(total_payment_value), 2)          AS avg_order_value,
    ROUND(MAX(total_payment_value), 2)          AS max_order_value,

    -- Review metrics
    ROUND(AVG(avg_review_score), 2)             AS avg_review_score,

    -- Cancellation rate
    ROUND(
        SAFE_DIVIDE(
            COUNTIF(is_canceled),
            COUNT(DISTINCT order_id)
        ) * 100, 2
    )                                           AS cancellation_rate_pct,

    -- Delivery rate
    ROUND(
        SAFE_DIVIDE(
            COUNTIF(is_delivered),
            COUNT(DISTINCT order_id)
        ) * 100, 2
    )                                           AS delivery_rate_pct,

    -- Revenue per customer
    ROUND(
        SAFE_DIVIDE(
            SUM(total_payment_value),
            COUNT(DISTINCT customer_trx_id)
        ), 2
    )                                           AS revenue_per_customer

FROM customer_orders
GROUP BY
    customer_country,
    customer_country_code,
    customer_city,
    customer_postal_code,
    geo_lat,
    geo_lon,
    sale_year,
    sale_month,
    sale_yearmonth
