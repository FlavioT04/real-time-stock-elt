from sqlalchemy import create_engine, text
from scripts.config import DB_URI
from pathlib import Path

def transform():
    # db connection
    engine = create_engine(DB_URI)

    # get sql code
    sql_path = Path(__file__).parents[1] / 'sql' / 'stock_transform.sql'
    with open(sql_path, 'r') as f:
        sql = f.read()

    # execute sql code
    with engine.connect() as conn:
        conn.execute(text(sql))
        conn.commit()

    print('stock data transformed')