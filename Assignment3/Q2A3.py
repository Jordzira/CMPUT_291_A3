# goal was to create a comparable table and then use it in the bottom extended from Q1. Currently getting a syntax error around the CREATE VIEW section with this iteration
-- Common Expression Table (CTE) for grouping the order_id and the number of items/group as group_count
WITH cte_group_count AS (
	SELECT order_id, COUNT(order_item_id) AS group_count
	FROM Order_items
	GROUP BY order_id
),
-- CTE to find the average number of items per order
cte_avg AS (
	SELECT AVG(group_count) AS total_average
	FROM cte_group_count
)
-- Creating the view where the order_size is greater than the total_average
CREATE VIEW OrderSize(oid, size)
AS SELECT O.order_id AS oid, COUNT(O.order_item_id) AS order_size
FROM Order_items AS O
JOIN cte_avg AS A ON size > total_average
GROUP BY O.order_id;
