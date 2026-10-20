-- Q5 (ELT): top 10 customers by spend, with loyalty tier. Only the masked name is shown (no email or phone).
SELECT spend_rank, customer_id, name_masked, loyalty_tier, delivered_orders, total_spend
FROM mart_top_customers
WHERE spend_rank <= 10
ORDER BY spend_rank;
