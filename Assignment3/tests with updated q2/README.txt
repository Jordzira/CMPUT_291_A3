Group Number: 69
Names (CCID): Josh William (jdwillia), Marcus McLean (mcmclean), Jordan Atkins (atkins3)

Resources Used: 

"We declare that we did not collaborate with anyone outside our own group in this assignment"

User Optimized Report:

Three Scenarios:
From talks with a TA, it is our understanding that the primary differences between the three scenarios (not mentioned
in the assignment guide)  is uninformed has SQLite’s auto index and foreign keys disabled, and both self-informed and
user-optimized have auto index and foreign keys enabled. 

Note for Q4:
A "customer with more than one order" is assumed to be a customer with 1 order with multiple items in that order. 
This assumption is based off a TA's answer in the class discord server.

Query Plan:
For each of our queries we tried to use covering indexes, so data could be obtained without having to traverse the 
entire databases to attain the necessary information. 

For query 1, we chose to index as such: 
-	Customers(customer_id, customer_postal_code)
-	Orders(order_id)
-	Order_items(order_id, order_item_id)

For Query 2, we chose:
-	Customers(customer_postal_code, customer_id)
-	Orders(order_id, customer_id)
-	Order_items(order_id, order_item_id)

For Query 3:
-	Customers(customer_id, customer_postal_code)
-	Orders(order_id, customer_id)
-	Order_items(order_item_id, order_id)

Query 4:
-	 Sellers(seller_id, seller_postal_code)
-	 Orders(order_id, customer_id)
-	 Order_items(order_id, order_item_id, seller_id)

The reasoning for these indexes, and the order they’re implemented, is we tried to use a bottom-up approach. 
Knowing queries get evaluated in the order (1) FROM, (2) WHERE, and (3) SELECT, we prioritized the indexes in the 
order the query would evaluate them. 
