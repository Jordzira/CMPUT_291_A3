import csv
import sqlite3
import random
import os

connection = None
cursor = None

# Get path directory of Python script
script_dir = os.path.dirname(os.path.abspath(__file__))

# Define the database filename
DB_FILENAME = os.path.join(script_dir, "A3Small.db")

# Define the CSV filename
CSV_FILENAME = os.path.join(script_dir, "olist_customers_dataset.csv")

# Define the number of tuples to sample
NUM_SAMPLES = 10000

# Connect to the database and create the Customers table
def connect(DB_FILENAME):
    global connection, cursor 
    connection = sqlite3.connect(DB_FILENAME)
    cursor = connection.cursor()
    cursor.execute(' PRAGMA foreign_keys=ON; ')
    connection.commit()
    return

# Create table using query in Assign 1
def create_table():
    global connection, cursor 
    cursor.execute('''
                    CREATE TABLE Customers(
                        customer_id TEXT, 
                        customer_postal_code INTEGER, 
                        PRIMARY KEY(customer_id));
                        ''')
    return

# Open the CSV file and read the data into a list
def insert_data():
    with open(CSV_FILENAME, "r") as f:
        reader = csv.reader(f)
        data = list(reader)

# Sample the data and insert it into the database
    samples = random.sample(data[1:], NUM_SAMPLES)
    for sample in samples:
        customer_id = sample[0]
        customer_postal_code = int(sample[2])
        cursor.execute("INSERT INTO Customers (customer_id, customer_postal_code) VALUES (?, ?)",
              (customer_id, customer_postal_code))

    connection.commit()
    print(f"{NUM_SAMPLES} random tuples inserted into {DB_FILENAME}") 
    return       



def main():
    global connection, cursor 
    connect(DB_FILENAME)
    create_table()
    insert_data()
    connection.close()


if __name__ == "__main__":
    main()