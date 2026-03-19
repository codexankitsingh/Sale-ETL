-- ═══════════════════════════════════════════════════════════
-- View : vw_review_sentiment
-- Dataset : {analytics_dataset}
-- Purpose : Review scores with order delivery context,
--           sentiment bucketing and late+unhappy flagging
-- Tables  : order_reviews + orders + order_payments
-- ═══════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW `{project}.{analytics_dataset}.vw_review_sentiment` AS

WITH

-- ── Payment aggregates per order ──────────────────────────
payment_agg AS (
    SELECT
        order_id,
        ROUND(SUM(payment_value), 2)            AS total_payment_value,
        ARRAY_AGG(
            payment_type
            ORDER BY payment_value DESC
            LIMIT 1
        )[OFFSET(0)]                            AS primary_payment_type
    FROM `{project}.{raw_dataset}.order_payments`
    GROUP BY order_id
),

-- ── Orders with delivery metrics ──────────────────────────
order_enriched AS (
    SELECT
        o.order_id,
        o.customer_trx_id,
        o.order_status,
        o.sale_date,
        EXTRACT(YEAR  FROM o.sale_date)         AS sale_year,
        EXTRACT(MONTH FROM o.sale_date)         AS sale_month,
        FORMAT_DATE('%Y-%m', o.sale_date)       AS sale_yearmonth,

        -- Delivery flags
        o.order_status = 'delivered'            AS is_delivered,
        o.order_status = 'canceled'             AS is_canceled,

        -- Delivery delay days
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
        p.primary_payment_type

    FROM `{project}.{raw_dataset}.orders` o
    LEFT JOIN payment_agg p ON o.order_id = p.order_id
)

-- ── Final SELECT ──────────────────────────────────────────
SELECT
    -- Review identifiers
    r.review_id,
    r.order_id,

    -- Review content
    r.review_score,
    r.review_comment_title_en,
    r.review_comment_message_en,
    r.review_creation_date,
    r.review_answer_timestamp,
    r.review_sentiment,

    -- Response time in hours
    ROUND(
        TIMESTAMP_DIFF(
            r.review_answer_timestamp,
            r.review_creation_date,
            HOUR
        ), 0
    )                                           AS review_response_hours,

    -- Sentiment bucket from score
    CASE
        WHEN r.review_score >= 4 THEN 'Positive'
        WHEN r.review_score  = 3 THEN 'Neutral'
        ELSE                          'Negative'
    END                                         AS sentiment_bucket,

    -- Star label
    CASE r.review_score
        WHEN 5 THEN '⭐⭐⭐⭐⭐ Excellent'
        WHEN 4 THEN '⭐⭐⭐⭐ Good'
        WHEN 3 THEN '⭐⭐⭐ Average'
        WHEN 2 THEN '⭐⭐ Poor'
        WHEN 1 THEN '⭐ Very Poor'
        ELSE        'No Rating'
    END                                         AS star_label,

    -- Order context
    o.customer_trx_id,
    o.order_status,
    o.sale_date,
    o.sale_year,
    o.sale_month,
    o.sale_yearmonth,
    o.is_delivered,
    o.is_canceled,
    o.delivery_delay_days,
    o.shipping_days,

    -- Late + unhappy flag
    CASE
        WHEN COALESCE(o.delivery_delay_days, 0) > 0
         AND r.review_score <= 2
        THEN TRUE
        ELSE FALSE
    END                                         AS is_late_and_unhappy,

    -- Early delivery + happy flag
    CASE
        WHEN COALESCE(o.delivery_delay_days, 0) < 0
         AND r.review_score >= 4
        THEN TRUE
        ELSE FALSE
    END                                         AS is_early_and_happy,

    -- Payment context
    o.total_payment_value,
    o.primary_payment_type,

    r.etl_timestamp

FROM `{project}.{raw_dataset}.order_reviews` r
LEFT JOIN order_enriched o ON r.order_id = o.order_id
