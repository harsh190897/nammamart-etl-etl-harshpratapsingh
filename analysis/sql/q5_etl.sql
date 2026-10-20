-- Q5 (ETL): top 10 customers by spend, with loyalty tier. Only the masked name is shown (no email or phone).
SELECT row_number() OVER (ORDER BY sum(f.net_revenue) DESC, f.customer_id) AS spend_rank,
       f.customer_id, c.name_masked, c.loyalty_tier,
       count(*) AS delivered_orders, round(sum(f.net_revenue), 2) AS total_spend
FROM etl_fact_orders AS f
JOIN etl_dim_customer AS c USING (customer_id)
WHERE f.order_status = 'DELIVERED'
GROUP BY f.customer_id, c.name_masked, c.loyalty_tier
ORDER BY spend_rank
LIMIT 10;
