# need to set scenario:

# code for tracking time
start = time.time()
# execute Q1 50 times
for q in range(0,50):
    # randomly select a random customer with >1 orders
    cursor.execute('''SELECT Or1.customer_id
                        FROM Orders Or1, Orders Or2
                        WHERE Or1.customer_id = Or2.customer_id AND
                        Or1.order_id <> Or2.order_id
                    ORDER BY RANDOM()
                    LIMIT 1;''')
    random_customer = cursor.fetchone()

    cursor.execute('''SELECT COUNT( DISTINCT(S.seller_postal_code))
                    FROM Sellers S, Orders O, Order_items Oi
                    WHERE O.customer_id =:random_customer AND
                        O.order_id = Oi.order_id AND
                        Oi.seller_id = S.seller_id;''', {"random_customer":random_customer[0]})
# get time for this scenario/database combination
end = time.time()
execution_time = start - end
# need to append time to each scenario's array of times:

# disconnect from db and reconnect
conn.close()
conn = sqlite3.connect(DB_FILENAME)
cursor = conn.cursor()