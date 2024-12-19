# connect.py

import mysql.connector

def create_connection():
    """Create and return a connection to the MySQL database."""
    conn = mysql.connector.connect(
        host="localhost",  # Change to your MySQL host
        user="root",       # Change to your MySQL user
        password="2025",       # Change to your MySQL password
        database="metadb"  # Change to your target database name
    )
    return conn
