from fastapi import FastAPI, Request
import sqlite3, config
from fastapi.templating import Jinja2Templates
from datetime import date

app = FastAPI()
templates = Jinja2Templates(directory="templates")

@app.get("/")
def index(request: Request):
    stock_filter = request.query_params.get('filter',False)

    connection = sqlite3.connect(config.DB_FILE)
    connection.row_factory = sqlite3.Row

    cursor = connection.cursor()

    # df = pd.read_sql_query("Select * from stock", connection)
    # print(df)
    if stock_filter == 'new_closing_highs':
        cursor.execute("""
            SELECT * FROM (
                SELECT symbol, name, stock_id, max(close),date
                FROM stock_price 
                JOIN stock ON stock.id = stock_price.stock_id
                GROUP BY stock_id
                ORDER BY symbol
            ) WHERE date = (SELECT MAX(date) FROM stock_price)
        """)
    else:
        cursor.execute("""
            SELECT id, symbol, name FROM stock ORDER BY SYMBOL
            """)

    if stock_filter == 'new_closing_lows':
        cursor.execute("""
            SELECT * FROM (
                SELECT symbol, name, stock_id, min(close),date
                FROM stock_price 
                JOIN stock ON stock.id = stock_price.stock_id
                GROUP BY stock_id
                ORDER BY symbol
            ) WHERE date = (SELECT MAX(date) FROM stock_price)
        """)
    else:
        cursor.execute("""
            SELECT id, symbol, name FROM stock ORDER BY SYMBOL
            """)
    
    rows = cursor.fetchall()

    return templates.TemplateResponse("index.html", {"request": request, "stocks": rows})

@app.get("/stock/{symbol}")
def stock_detail(request: Request, symbol):
    print(dir(request))
    connection = sqlite3.connect(config.DB_FILE)
    connection.row_factory = sqlite3.Row

    cursor = connection.cursor()

    # df = pd.read_sql_query("Select * from stock", connection)
    # print(df)

    cursor.execute("""
        SELECT id, symbol, name FROM stock WHERE symbol = ?
        """, (symbol,))
    
    row = cursor.fetchone()

    cursor.execute("""
    SELECT * FROM stock_price WHERE stock_id = ? ORDER BY date DESC
    """, (row['id'],))

    bars = cursor.fetchall()

    return templates.TemplateResponse("stock_detail.html", {"request": request, "stock": row, "bars": bars})
# uvicorn main:app --reload 