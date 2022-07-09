import sqlite3, config
import alpaca_trade_api as tradeapi
import pandas as pd

connection = sqlite3.connect(config.DB_FILE)
connection.row_factory = sqlite3.Row

cursor = connection.cursor()

# df = pd.read_sql_query("Select * from stock", connection)
# print(df)

cursor.execute("""
    SELECT symbol, name FROM stock
    """)

rows = cursor.fetchall()
symbols = [row['symbol'] for row in rows]
print(symbols)

# cursor.execute("INSERT INTO stock (symbol, company) VALUES ('VZ','Verizon')")
# cursor.execute("INSERT INTO stock (symbol, company) VALUES ('Z',' Zillow')")
api = tradeapi.REST(config.API_KEY, config.SECRET_KEY, base_url = config.API_URL)
assets = api.list_assets()

for asset in assets: 
    try:
        if asset.symbol not in symbols and asset.status == 'active' and asset.tradable:
            print(f"Added a new stock {asset.symbol} {asset.name}")
            cursor.execute("INSERT INTO stock (symbol, name, exchange) VALUES (?,?,?)", (asset.symbol, asset.name, asset.exchange))
    except Exception as e:
        print(asset.symbol)
        print(e)

connection.commit() 