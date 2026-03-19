-- ═══════════════════════════════════════════════════════════
-- View : vw_seller_performance
-- Dataset : {analytics_dataset}
-- Purpose : Seller level revenue, delivery and review metrics
--           with geo enrichment for mapping
-- Tables  : sellers + order_items + orders
--           + order_reviews + geolocations
-- ═══════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW `{project}.{analytics_dataset}.vw_seller_performance` AS

WITH

-- ── Item aggregates per seller per order ──────────────────
seller_items AS (
    SELECT
        oi.seller_id,
        oi.order_id,
        COUNT(oi.order_item_id)                 AS items_in_order,
        ROUND(SUM(oi.price), 2)                 AS order_revenue,
        ROUND(SUM(oi.freight_value), 2)         AS order_freight,
        ROUND(AVG(oi.price), 2)                 AS avg_item_price
    FROM `{project}.{raw_dataset}.order_items` oi
    GROUP BY oi.seller_id, oi.order_id
),

-- ── Orders with delivery metrics ──────────────────────────
order_metrics AS (
    SELECT
        order_id,
        order_status = 'delivered'              AS is_delivered,
        order_status = 'canceled'               AS is_canceled,
        CASE
            WHEN order_delivered_customer_date IS NOT NULL
             AND order_estimated_delivery_date IS NOT NULL
            THEN DATE_DIFF(
                DATE(order_delivered_customer_date),
                DATE(order_estimated_delivery_date),
                DAY
            )
            ELSE NULL
        END                                     AS delivery_delay_days,
        CASE
            WHEN order_delivered_customer_date IS NOT NULL
             AND order_delivered_carrier_date  IS NOT NULL
            THEN DATE_DIFF(
                DATE(order_delivered_customer_date),
                DATE(order_delivered_carrier_date),
                DAY
            )
            ELSE NULL
        END                                     AS shipping_days
    FROM `{project}.{raw_dataset}.orders`
),

-- ── Review aggregates per order ───────────────────────────
review_agg AS (
    SELECT
        order_id,
        ROUND(AVG(review_score), 2)             AS avg_review_score,
        COUNT(review_id)                        AS total_reviews
    FROM `{project}.{raw_dataset}.order_reviews`
    GROUP BY order_id
),

-- ── Seller full aggregates ────────────────────────────────
seller_agg AS (
    SELECT
        si.seller_id,
        COUNT(DISTINCT si.order_id)             AS total_orders,
        SUM(si.items_in_order)                  AS total_items_sold,
        ROUND(SUM(si.order_revenue), 2)         AS total_revenue,
        ROUND(SUM(si.order_freight), 2)         AS total_freight,
        ROUND(AVG(si.avg_item_price), 2)        AS avg_item_price,
        ROUND(AVG(r.avg_review_score), 2)       AS avg_review_score,
        SUM(r.total_reviews)                    AS total_reviews,
        COUNTIF(om.is_delivered)                AS delivered_orders,
        COUNTIF(om.is_canceled)                 AS canceled_orders,
        ROUND(AVG(om.delivery_delay_days), 2)   AS avg_delivery_delay_days,
        ROUND(AVG(om.shipping_days), 2)         AS avg_shipping_days
    FROM seller_items si
    LEFT JOIN order_metrics om ON si.order_id = om.order_id
    LEFT JOIN review_agg    r  ON si.order_id = r.order_id
    GROUP BY si.seller_id
),

-- ── Geo lookup ────────────────────────────────────────────
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
    s.seller_id,
    s.seller_name,
    s.seller_city,
    s.seller_country,
    s.country_code,
    s.seller_postal_code,

    -- Geo enrichment
    g.geo_lat,
    g.geo_lon,

    -- Sales metrics
    COALESCE(a.total_orders, 0)                 AS total_orders,
    COALESCE(a.total_items_sold, 0)             AS total_items_sold,
    COALESCE(a.total_revenue, 0)                AS total_revenue,
    COALESCE(a.total_freight, 0)                AS total_freight,
    COALESCE(a.avg_item_price, 0)               AS avg_item_price,

    -- Review metrics
    a.avg_review_score,
    COALESCE(a.total_reviews, 0)                AS total_reviews,

    -- Delivery metrics
    COALESCE(a.delivered_orders, 0)             AS delivered_orders,
    COALESCE(a.canceled_orders, 0)              AS canceled_orders,
    a.avg_delivery_delay_days,
    a.avg_shipping_days,

    -- Seller performance tier
    CASE
        WHEN COALESCE(a.total_revenue, 0) >= 50000 THEN 'Platinum'
        WHEN COALESCE(a.total_revenue, 0) >= 10000 THEN 'Gold'
        WHEN COALESCE(a.total_revenue, 0) >= 1000  THEN 'Silver'
        ELSE                                             'Bronze'
    END                                         AS seller_tier,

    -- Cancellation rate
    ROUND(
        SAFE_DIVIDE(
            COALESCE(a.canceled_orders, 0),
            COALESCE(a.total_orders, 1)
        ) * 100, 2
    )                                           AS cancellation_rate_pct,

    s.etl_timestamp

FROM `{project}.{raw_dataset}.sellers` s
LEFT JOIN seller_agg a  ON s.seller_id       = a.seller_id
LEFT JOIN geo_lookup g  ON s.seller_postal_code = g.geo_postal_code
