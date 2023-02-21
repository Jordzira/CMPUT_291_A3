import csv
import sqlite3
import random
import os

conn = None
cursor = None

# Get path directory of Python script
script_dir = os.path.dirname(os.path.abspath(__file__))

# Define the database filename
DB_FILENAME = os.path.join(script_dir, "A3Large.db")

# Define the CSV filenames
CSV_FILENAME_CUSTOMERS = os.path.join(script_dir, "olist_customers_dataset.csv")
CSV_FILENAME_SELLERS = os.path.join(script_dir, "olist_sellers_dataset.csv")
CSV_FILENAME_ORDERS = os.path.join(script_dir, "olist_orders_dataset.csv")
CSV_FILENAME_ORDER_ITEMS = os.path.join(script_dir, "olist_order_items_dataset.csv")


# Connect to the database and create the Customers table
def connect(DB_FILENAME):
    global conn, cursor 
    conn = sqlite3.connect(DB_FILENAME)
    cursor = conn.cursor()
    cursor.execute(' PRAGMA foreign_keys=ON; ')
    conn.commit()
    return

# Create table using query in Assign 1
def create_table():
    global conn, cursor 
    cursor.execute('''
                    CREATE TABLE Customers(
                        customer_id TEXT, 
                        customer_postal_code INTEGER, 
                        PRIMARY KEY(customer_id));
                        ''')

    cursor.execute('''
                    CREATE TABLE Sellers(
                        seller_id TEXT, 
                        seller_postal_code INTEGER, 
                        PRIMARY KEY(seller_id));
                        ''')
    
    cursor.execute('''
                    CREATE TABLE Orders(
                        order_id TEXT, 
                        customer_id TEXT, 
                        PRIMARY KEY(order_id),
                        FOREIGN KEY(customer_id) REFERENCES Customers(customer_id));
                        ''')
    
    cursor.execute('''
                    CREATE TABLE Order_items(
                        order_id TEXT, 
                        order_item_id INTEGER, 
                        product_id TEXT, 
                        seller_id TEXT, 
                        PRIMARY KEY(order_id, order_item_id, product_id, seller_id),
                        FOREIGN KEY(seller_id) REFERENCES Sellers(seller_id),
                        FOREIGN KEY(order_id) REFERENCES Orders(order_id));
                        ''')
    return

# Open the olist_customers_dataset.csv file and read the data into a list
def insert_data():
    Customer_Samples = 33000
    with open(CSV_FILENAME_CUSTOMERS, "r") as f:
        reader = csv.reader(f)
        data = list(reader)

# Sample the data and insert it into the database
    samples = random.sample(data[1:], Customer_Samples)
    for sample in samples:
        customer_id = sample[0]
        customer_postal_code = int(sample[2])
        cursor.execute("INSERT INTO Customers (customer_id, customer_postal_code) VALUES (?, ?)",
              (customer_id, customer_postal_code))
    print(f"{Customer_Samples} random tuples inserted into 'Customers' table")

    conn.commit()
    print(f"{Customer_Samples} random tuples inserted into {DB_FILENAME}") 

# Open the olist_sellers_dataset.csv file and read the data into a list
    with open(CSV_FILENAME_SELLERS, "r") as f:
        reader = csv.reader(f)
        data = list(reader)

# Sample the data and insert it into the Sellers table
    Sellers_Samples = 1000
    samples = random.sample(data[1:], Sellers_Samples)
    for sample in samples:
        order_id = sample[0]
        customer_id = sample[1]
        cursor.execute("INSERT INTO Sellers (seller_id, seller_postal_code) VALUES (?, ?)",
              (order_id, customer_id))
    print(f"{Sellers_Samples} random tuples inserted into 'Sellers' table")

    conn.commit()
    print(f"{Sellers_Samples} random tuples inserted into {DB_FILENAME}") 

# Store all customer_id
    cursor.execute("SELECT customer_id FROM Customers")
    rows = cursor.fetchall()
    customer_ids = set(row[0] for row in rows)

# Open the olist_orders_dataset.csv file and process it line by line
    order_samples = 0
    with open(CSV_FILENAME_ORDERS, "r") as f:
        reader = csv.reader(f)
        next(reader)  # skip header row
        for row in reader:
            customer_id = row[1]
            if customer_id in customer_ids:
                order_id = row[0]
                conn.execute("INSERT INTO Orders (order_id, customer_id) VALUES (?, ?)", (order_id, customer_id,))
                order_samples += 1
    
    conn.commit()

    print(f"{order_samples} random tuples inserted into {DB_FILENAME}") 


# Store all order_ids and seller_ids that exist in the Orders and Sellers tables
    cursor.execute("SELECT order_id FROM Orders")
    rows = cursor.fetchall()
    order_ids = set(row[0] for row in rows)

    cursor.execute("SELECT seller_id FROM Sellers")
    rows = cursor.fetchall()
    seller_ids = set(row[0] for row in rows)

# Process the olist_order_items_dataset.csv file line by line
    order_items_samples = 0
    with open(CSV_FILENAME_ORDER_ITEMS, "r") as f:
        reader = csv.reader(f)
        next(reader)  # skip header row
        for row in reader:
            order_id = row[0]
            seller_id = row[3]
            if order_id in order_ids and seller_id in seller_ids:
                conn.execute("INSERT INTO Order_items (order_id, order_item_id, product_id, seller_id) VALUES (?, ?, ?, ?)",
                         (order_id, int(row[1]), row[2], seller_id))
                order_items_samples += 1

    conn.commit()

    print(f"{order_items_samples} random tuples inserted into {DB_FILENAME}")

    return       



def main():
    global conn, cursor 
    connect(DB_FILENAME)
    create_table()
    insert_data()
    conn.close()


if __name__ == "__main__":
    main()