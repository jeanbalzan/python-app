import sqlite3, config
from alpaca_trade_api.rest import TimeFrame
import alpaca_trade_api as tradeapi
import config

connection = sqlite3.connect(config.DB_FILE)

connection.row_factory = sqlite3.Row

cursor = connection.cursor()

cursor.execute("""
    SELECT id, symbol FROM stock
    """)

rows = cursor.fetchall()

# symbols = [row['symbol'] for row in rows]

api = tradeapi.REST(config.API_KEY, config.SECRET_KEY, base_url = config.API_URL,api_version = 'v2')
symbols = []
stock_dict = {}

for row in rows:
    symbol = row['symbol']
    symbols.append(symbol)
    stock_dict[symbol] = row['id']

chunk_size = 200 

for i in range(0, len(symbols), chunk_size):
    symbol_chunk = symbols[i:i+chunk_size]

    barsets = api.get_bars(symbol_chunk,TimeFrame.Day,"2022-01-31", "2022-03-31")

    for bar in barsets:
        symbol = bar.S
        print(f"processing symbol {symbol}")

        stock_id = stock_dict[symbol]
        cursor.execute("""
            INSERT INTO stock_price (stock_id, date, open, high, low, close, volume)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (stock_id, bar.t.date(), bar.o, bar.h, bar.l, bar.c, bar.v))
        
connection.commit()
