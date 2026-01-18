import psycopg2
from sqlalchemy import create_engine
import os

def load_prices(df, table='cse_prices'):
    user = os.getenv("POSTGRES_USER")
    pwd = os.getenv("POSTGRES_PASSWORD")
    host = os.getenv("POSTGRES_HOST","localhost")
    port = os.getenv("POSTGRES_PORT","5432")
    db = os.getenv("POSTGRES_DB")
    url = f"postgresql://{user}:{pwd}@{host}:{port}/{db}"
    engine = create_engine(url)
    df.to_sql(table, engine, if_exists='append', index=False, method='multi', chunksize=1000)