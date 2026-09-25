-- Query 1: Top 10 Market Segments by Realized Revenue (Non-Canceled)
-- Business Metric: Identifies the highest value booking acquisition channels

SELECT 
    market_segment,
    COUNT(booking_id) AS total_confirmed_bookings,
    ROUND(SUM(adr * total_stay_nights), 2) AS total_revenue,
    ROUND(AVG(adr), 2) AS average_daily_rate
FROM hotel_bookings
WHERE is_canceled = 0
GROUP BY market_segment
ORDER BY total_revenue DESC
LIMIT 10;


-- Query 2: Monthly Revenue & Booking Growth Analysis
-- Business Metric: Month-over-Month growth trends in booking volume & revenue

WITH monthly_metrics AS (
    SELECT 
        DATE_TRUNC('month', arrival_date) AS booking_month,
        COUNT(booking_id) AS booking_count,
        SUM(adr * total_stay_nights) AS monthly_revenue
    FROM hotel_bookings
    WHERE is_canceled = 0
    GROUP BY DATE_TRUNC('month', arrival_date)
)
SELECT 
    TO_CHAR(booking_month, 'YYYY-MM') AS month_label,
    booking_count,
    ROUND(monthly_revenue, 2) AS revenue,
    LAG(booking_count) OVER (ORDER BY booking_month) AS prev_month_bookings,
    ROUND(
        (monthly_revenue - LAG(monthly_revenue) OVER (ORDER BY booking_month)) 
        / NULLIF(LAG(monthly_revenue) OVER (ORDER BY booking_month), 0) * 100, 
        2
    ) AS revenue_mom_growth_pct
FROM monthly_metrics
ORDER BY booking_month;


-- Query 3: Geographical Performance & Cancellation Rate by Country
-- Business Metric: International market share, average rate, and risk ratio

SELECT 
    country,
    COUNT(booking_id) AS total_bookings,
    ROUND(AVG(adr), 2) AS avg_adr,
    SUM(CASE WHEN is_canceled = 1 THEN 1 ELSE 0 END) AS canceled_bookings,
    ROUND(
        (SUM(CASE WHEN is_canceled = 1 THEN 1.0 ELSE 0 END) / COUNT(booking_id)) * 100, 
        2
    ) AS cancellation_rate_pct
FROM hotel_bookings
WHERE country != 'UNK'
GROUP BY country
HAVING COUNT(booking_id) >= 50
ORDER BY total_bookings DESC
LIMIT 15;