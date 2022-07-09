import sqlite3, config

from psycopg2 import connect

connection = sqlite3.connect(config.DB_FILE)

cursor = connection.cursor()

cursor.execute("""
    CREATE TABLE IF NOT EXISTS stock (
        id INTEGER PRIMARY KEY,
        symbol TEXT NOT NULL UNIQUE,
        name TEXT NOT NULL,
        exchange TEXT NOT NULL
    )
""")

cursor.execute("""
    CREATE TABLE IF NOT EXISTS stock_price (
        id INTEGER PRIMARY KEY,
        stock_id INTEGER,
        date NOT NULL,
        open NOT NULL,
        high NOT NULL,
        low NOT NULL,
        close NOT NULL,
        volume NOT NULL,
        FOREIGN KEY (stock_id) REFERENCES stock (id)
    )
""")

cursor.execute("""
    CREATE TABLE IF NOT EXISTS stock_price_minute (
        id INTEGER PRIMARY KEY, 
        stock_id INTEGER,
        datetime NOT NULL,
        open NOT NULL, 
        high NOT NULL,
        low NOT NULL, 
        close NOT NULL, 
        volume NOT NULL,
        FOREIGN KEY (stock_id) REFERENCES stock (id)
        )
""")

connection.commit()