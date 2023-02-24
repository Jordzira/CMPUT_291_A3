import csv
import sqlite3
import random
import os

conn = None
cursor = None

# Get path directory of Python script
script_dir = os.path.dirname(os.path.abspath(__file__))

# Define the database filename
DB_FILENAME = os.path.join(script_dir, "A3Small.db")

# Define the CSV filenames
CSV_FILENAME_CUSTOMERS = os.path.join(script_dir, "olist_customers_dataset.csv")
CSV_FILENAME_ORDER_SELLERS = os.path.join(script_dir, "olist_sellers_dataset.csv")
CSV_FILENAME_ORDERS = os.path.join(script_dir, "olist_orders_dataset.csv")
CSV_FILENAME_ORDER_ITEMS = os.path.join(script_dir, "olist_order_items_dataset.csv")


# Connect to the database and create the Customers table
def connect(DB_FILENAME):
    global conn, cursor 
    conn = sqlite3.connect(DB_FILENAME)
    cursor = conn.cursor()
    # disable (smart) creation of index
    cursor.execute(' PRAGMA foreign_keys=FALSE; ')
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
    # creating indexes
    cursor.execute("CREATE INDEX customers_index ON Customers(customer_id);")
    cursor.execute("CREATE INDEX sellers_index ON Sellers(seller_id);")
    cursor.execute("CREATE INDEX orders_index ON Orders(order_id);")
    cursor.execute("CREATE INDEX order_items_index ON Order_items(order_id, order_item_id, product_id, seller_id);")
    
    return

# Open the olist_customers_dataset.csv file and read the data into a list
def insert_data():
    Customer_Samples = 10000
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

# Open the olist_orders_dataset.csv file and read the data into a list
    with open(CSV_FILENAME_ORDERS, "r") as f:
        reader = csv.reader(f)
        data = list(reader)

# Sample the data and insert it into the Sellers table
    Sellers_Samples = 500
    samples = random.sample(data[1:], Sellers_Samples)
    for sample in samples:
        order_id = sample[0]
        customer_id = sample[1]
        cursor.execute("INSERT INTO Sellers (seller_id, seller_postal_code) VALUES (?, ?)",
              (order_id, customer_id))
    print(f"{Sellers_Samples} random tuples inserted into 'Sellers' table")

    conn.commit()
    print(f"{Sellers_Samples} random tuples inserted into {DB_FILENAME}") 
    
    return       



def main():
    global conn, cursor 
    connect(DB_FILENAME)
    create_table()
    insert_data()
    conn.close()


if __name__ == "__main__":
    main()
