import csv
import sqlite3
import random
import os
import time
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np

conn = None
cursor = None

# Get path directory of Python script
script_dir = os.path.dirname(os.path.abspath(__file__))

# Graph 
def graph(uninformed_times, self_optimized_times, user_optimized_times):
    databases = ( 
        "SmallDB",
        "MediumDB",
        "LargeDB",
    )
    # need to track time and store it in the variables in the array so it can plot bars accordingly
    User_Optimized = np.array([user_optimized_times[0], user_optimized_times[1], user_optimized_times[2]])
    Self_Optimized = np.array([self_optimized_times[0], self_optimized_times[1], self_optimized_times[2]])
    Uninformed = np.array([uninformed_times[0], uninformed_times[1], uninformed_times[2]])

    plt.bar(databases, User_Optimized, color = 'r')
    plt.bar(databases, Self_Optimized, bottom = User_Optimized, color = 'b')
    plt.bar(databases, Uninformed, bottom = User_Optimized + Self_Optimized, color = 'g')
    
    plt.xlabel("Databases")
    plt.ylabel("Time in Seconds")
    plt.legend(["User Optimized","Self Optimized", "Uninformed"])
    plt.title("Query 4 (runtime in seconds)")
    
    plt.savefig("Q4A3chart.png")

# Connect to the database and create the Customers table
def connect(DB_FILENAME):
    global conn, cursor 
    conn = sqlite3.connect(DB_FILENAME)
    cursor = conn.cursor()
    conn.commit()
    return     

def auto_index_and_fkeys(index_choice, key_choice):
    # alter auto index
    global conn, cursor 
    
    if index_choice == 'on':
        cursor.execute("PRAGMA automatic_index = ON;")
    elif index_choice == 'off':
        cursor.execute("PRAGMA automatic_index = OFF;")
        
    if key_choice == 'on':
        cursor.execute(' PRAGMA foreign_keys=ON; ')
    elif key_choice == 'off':
        cursor.execute(' PRAGMA foreign_keys=OFF; ')
    
    
    conn.commit()
def add_keys():
    # add primary and foreign keys for each table
    global conn, cursor 

    cursor.execute("DROP TABLE IF EXISTS Old_Customers;")
    cursor.execute("DROP TABLE IF EXISTS Old_Sellers;")
    cursor.execute("DROP TABLE IF EXISTS Old_Orders;")
    cursor.execute("DROP TABLE IF EXISTS Old_Order_items;")

    # Customers
    cursor.execute("ALTER TABLE Customers RENAME TO Old_Customers;")
    cursor.execute('''CREATE TABLE Customers(
                        customer_id TEXT, 
                        customer_postal_code INTEGER,
                        PRIMARY KEY (customer_id));
                        ''')
    cursor.execute("INSERT INTO Customers SELECT * FROM Old_Customers;")
    
    # Sellers
    cursor.execute("ALTER TABLE Sellers RENAME TO Old_Sellers;")
    cursor.execute('''
                    CREATE TABLE Sellers(
                        seller_id TEXT, 
                        seller_postal_code INTEGER, 
                        PRIMARY KEY(seller_id));
                        ''')
    cursor.execute("INSERT INTO Sellers SELECT * FROM Old_Sellers;")
    
    # Orders
    cursor.execute("ALTER TABLE Orders RENAME TO Old_Orders;")
    cursor.execute('''
                    CREATE TABLE Orders(
                        order_id TEXT, 
                        customer_id TEXT, 
                        PRIMARY KEY(order_id),
                        FOREIGN KEY(customer_id) REFERENCES Customers(customer_id));
                        ''')
    
    cursor.execute("INSERT INTO Orders SELECT * FROM Old_Orders;")
    
    # Order Items
    cursor.execute("ALTER TABLE Order_items RENAME TO Old_Order_items;")
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
    cursor.execute("INSERT INTO Order_items SELECT * FROM Old_Order_items;")

    
    conn.commit()

def create_indexes():
    # create indexes, for each table, for user-optimized portion
    global conn, cursor 
    cursor.execute("DROP INDEX IF EXISTS customers_index;")
    cursor.execute("DROP INDEX IF EXISTS sellers_index;")
    cursor.execute("DROP INDEX IF EXISTS orders_index;")
    cursor.execute("DROP INDEX IF EXISTS order_items_index;")

    cursor.execute("CREATE INDEX sellers_index ON Sellers(seller_id, seller_postal_code);")
    cursor.execute("CREATE INDEX orders_index ON Orders(order_id, customer_id);")
    cursor.execute("CREATE INDEX order_items_index ON Order_items(order_id, order_item_id, seller_id);")
    
    conn.commit()


def execute_Q4():
    global conn, cursor
    # execute Q4 50 times
    for q in range(0,50):
    # randomly select a random customer with >1 items in their order
        cursor.execute('''SELECT DISTINCT O.customer_id
                            FROM Orders O, Order_items Oi
                            WHERE O.order_id = Oi.order_id AND
                                Oi.order_item_id > 1
                            ORDER BY RANDOM()
                            LIMIT 1;''')
        random_customer = cursor.fetchone()

        cursor.execute('''SELECT COUNT( DISTINCT(S.seller_postal_code))
                        FROM Sellers S, Orders O, Order_items Oi
                        WHERE O.customer_id =:random_customer AND
                            O.order_id = Oi.order_id AND
                            Oi.seller_id = S.seller_id;''', {"random_customer":random_customer[0]})
    return 

def reconnect(DB_FILENAME):
    global conn, cursor
    conn.close()
    conn = sqlite3.connect(DB_FILENAME)
    cursor = conn.cursor()
    return

def main():
    global conn, cursor 
    # list of database filenames
    db_names = ["A3Small.db", "A3Medium.db", "A3Large.db"]

    # create list of execution times for each scenario, used to create "experiments" dict in the graph.py sample
    # where index 0 should be small's time, 1 medium, 2 large
    uninformed_times = []
    self_optimized_times = []
    user_optimized_times = []

    # iteration 0 = smalldb, 1 = medium, 2 = large 
    for db in range(0,3):
        # Define the database filename
        DB_FILENAME = os.path.join(script_dir, db_names[db])
        print(f"Connected to " + db_names[db])
        connect(DB_FILENAME)
        # set scenario uninformed
        auto_index_and_fkeys('off', 'off')
        cursor.execute("DROP VIEW IF EXISTS OrderSize;")
        # start time
        start = time.time()
        # runs scenario uninformed 50 times
        execute_Q4()
        # end time
        end = time.time()

        # average execution time
        ui_execution_time = (end - start)/50
        # append scenario to db's times list
        uninformed_times.append(ui_execution_time)

        # disconnect from db and reconnect to same database (minimize caching effects)
        reconnect(DB_FILENAME)

        ''' done scenario uninformed, start self-optimized '''

        # set scenario self-optimized
        add_keys()
        auto_index_and_fkeys('on', 'on')

        cursor.execute("DROP VIEW IF EXISTS OrderSize;")
        # start time
        start = time.time()
        # runs scenario uninformed 50 times
        execute_Q4()
        # end time
        end = time.time()

        # average execution time
        so_execution_time = (end - start)/50
        # append scenario to db's times list
        self_optimized_times.append(so_execution_time)

        # disconnect from db and reconnect to same database (minimize caching effects)
        reconnect(DB_FILENAME)

        ''' done scenario self-optimized, start user-optimized'''

        # set scenario user-optimized
        create_indexes()
        # start time
        cursor.execute("DROP VIEW IF EXISTS OrderSize;")
        start = time.time()
        # runs scenario uninformed 50 times
        execute_Q4()
        # end time
        end = time.time()

        # average execution time
        uo_execution_time = (end - start)/50
        # append scenario to db's times list
        user_optimized_times.append(uo_execution_time)

        # disconnect from database
        conn.close()
    
        print(uninformed_times[db], self_optimized_times[db], user_optimized_times[db])
    graph(uninformed_times, self_optimized_times, user_optimized_times)

if __name__ == "__main__":
    main()