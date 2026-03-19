-- ═══════════════════════════════════════════════════════════
-- View : vw_product_performance
-- Dataset : {analytics_dataset}
-- Purpose : Product level sales, pricing, freight and
--           review metrics with size/weight analysis
-- Tables  : products + order_items + orders + order_reviews
-- ═══════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW `{project}.{analytics_dataset}.vw_product_performance` AS

WITH

-- ── Review aggregates per order ───────────────────────────
review_agg AS (
    SELECT
        order_id,
        ROUND(AVG(review_score), 2)             AS avg_review_score,
        COUNT(review_id)                        AS total_reviews,
        COUNTIF(review_score >= 4)              AS positive_reviews,
        COUNTIF(review_score = 3)               AS neutral_reviews,
        COUNTIF(review_score <= 2)              AS negative_reviews
    FROM `{project}.{raw_dataset}.order_reviews`
    GROUP BY order_id
),

-- ── Order status lookup ───────────────────────────────────
order_status AS (
    SELECT
        order_id,
        order_status = 'delivered'              AS is_delivered,
        order_status = 'canceled'               AS is_canceled
    FROM `{project}.{raw_dataset}.orders`
),

-- ── Product sales aggregates ──────────────────────────────
product_agg AS (
    SELECT
        oi.product_id,
        COUNT(DISTINCT oi.order_id)             AS total_orders,
        COUNT(oi.order_item_id)                 AS total_units_sold,
        COUNT(DISTINCT oi.seller_id)            AS unique_sellers,
        ROUND(SUM(oi.price), 2)                 AS total_revenue,
        ROUND(AVG(oi.price), 2)                 AS avg_price,
        ROUND(MIN(oi.price), 2)                 AS min_price,
        ROUND(MAX(oi.price), 2)                 AS max_price,
        ROUND(SUM(oi.freight_value), 2)         AS total_freight,
        ROUND(AVG(oi.freight_value), 2)         AS avg_freight,
        ROUND(AVG(r.avg_review_score), 2)       AS avg_review_score,
        SUM(r.total_reviews)                    AS total_reviews,
        SUM(r.positive_reviews)                 AS positive_reviews,
        SUM(r.neutral_reviews)                  AS neutral_reviews,
        SUM(r.negative_reviews)                 AS negative_reviews,
        COUNTIF(os.is_delivered)                AS delivered_orders,
        COUNTIF(os.is_canceled)                 AS canceled_orders
    FROM `{project}.{raw_dataset}.order_items` oi
    LEFT JOIN order_status os ON oi.order_id = os.order_id
    LEFT JOIN review_agg   r  ON oi.order_id = r.order_id
    GROUP BY oi.product_id
)

-- ── Final SELECT ──────────────────────────────────────────
SELECT
    p.product_id,
    p.product_category_name,

    -- Physical attributes
    p.product_weight_gr,
    p.product_length_cm,
    p.product_height_cm,
    p.product_width_cm,
    p.product_volume_cm3,

    -- Weight bucket for analysis
    CASE
        WHEN p.product_weight_gr < 500   THEN 'Light (< 500g)'
        WHEN p.product_weight_gr < 2000  THEN 'Medium (500g-2kg)'
        WHEN p.product_weight_gr < 5000  THEN 'Heavy (2kg-5kg)'
        ELSE                                  'Very Heavy (5kg+)'
    END                                         AS weight_bucket,

    -- Volume bucket
    CASE
        WHEN p.product_volume_cm3 < 1000  THEN 'Small'
        WHEN p.product_volume_cm3 < 5000  THEN 'Medium'
        WHEN p.product_volume_cm3 < 20000 THEN 'Large'
        ELSE                                   'Extra Large'
    END                                         AS size_bucket,

    -- Sales metrics
    COALESCE(a.total_orders, 0)                 AS total_orders,
    COALESCE(a.total_units_sold, 0)             AS total_units_sold,
    COALESCE(a.unique_sellers, 0)               AS unique_sellers,
    COALESCE(a.total_revenue, 0)                AS total_revenue,
    COALESCE(a.avg_price, 0)                    AS avg_price,
    COALESCE(a.min_price, 0)                    AS min_price,
    COALESCE(a.max_price, 0)                    AS max_price,

    -- Freight metrics
    COALESCE(a.total_freight, 0)                AS total_freight,
    COALESCE(a.avg_freight, 0)                  AS avg_freight,

    -- Freight to price ratio
    ROUND(
        SAFE_DIVIDE(
            COALESCE(a.avg_freight, 0),
            NULLIF(COALESCE(a.avg_price, 0), 0)
        ) * 100, 2
    )                                           AS freight_to_price_pct,

    -- Review metrics
    a.avg_review_score,
    COALESCE(a.total_reviews, 0)                AS total_reviews,
    COALESCE(a.positive_reviews, 0)             AS positive_reviews,
    COALESCE(a.neutral_reviews, 0)              AS neutral_reviews,
    COALESCE(a.negative_reviews, 0)             AS negative_reviews,

    -- Delivery metrics
    COALESCE(a.delivered_orders, 0)             AS delivered_orders,
    COALESCE(a.canceled_orders, 0)              AS canceled_orders,

    -- Cancellation rate
    ROUND(
        SAFE_DIVIDE(
            COALESCE(a.canceled_orders, 0),
            COALESCE(a.total_orders, 1)
        ) * 100, 2
    )                                           AS cancellation_rate_pct,

    -- Product performance tier
    CASE
        WHEN COALESCE(a.total_revenue, 0) >= 10000 THEN 'Top Seller'
        WHEN COALESCE(a.total_revenue, 0) >= 1000  THEN 'Good Seller'
        WHEN COALESCE(a.total_revenue, 0) >= 100   THEN 'Low Seller'
        ELSE                                            'No Sales'
    END                                         AS product_tier,

    p.etl_timestamp

FROM `{project}.{raw_dataset}.products` p
LEFT JOIN product_agg a ON p.product_id = a.product_id
