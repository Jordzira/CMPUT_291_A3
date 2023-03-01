# need to set scenario:

# code for tracking time
start = time.time()
# execute Q1 50 times
for q in range(0,50):
    # randomly select a postal code
    cursor.execute('''SELECT customer_postal_code
                    FROM Customers
                    ORDER BY RANDOM()
                    LIMIT 1''')
    random_postal = cursor.fetchone()

    cursor.execute('''SELECT COUNT(O.order_id)
                    FROM Customers C, Orders O
                    WHERE C.customer_postal_code=:random_postal AND
                        C.customer_id = O.customer_id AND
                        (SELECT COUNT(Oi.order_item_id)
                        FROM Order_items Oi
                        WHERE O.order_id = Oi.order_id) >
                            (SELECT (CAST(COUNT(Oi2.order_item_id) as real) / CAST(COUNT(DISTINCT Oi2.order_id)as real))
                            FROM Order_items Oi2);''', {"random_postal":random_postal[0]})
# get time for this scenario/database combination
end = time.time()
execution_time = start - end
# need to append time to each scenario's array of times:

# disconnect from db and reconnect
conn.close()
conn = sqlite3.connect(DB_FILENAME)
cursor = conn.cursor()