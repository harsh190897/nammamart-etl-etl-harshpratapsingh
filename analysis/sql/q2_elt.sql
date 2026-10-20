-- Q2 (ELT): revenue and gross margin by product category (margin = revenue - quantity x cost_price).
SELECT category, delivered_orders, revenue, cost, gross_margin, margin_pct
FROM mart_category_margin
ORDER BY revenue DESC;
