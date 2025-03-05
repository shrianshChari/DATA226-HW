from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import datetime
import snowflake.connector
import requests

def return_snowflake_conn() -> snowflake.connector.cursor.SnowflakeCursor:
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')

    return hook.get_conn().cursor()


@task
def extract(symbol: str):
    av_api = Variable.get('alpha_vantage_api_key')

    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={av_api}'

    resp = requests.get(url)
    data = resp.json()

    return data

@task
def transform(data: dict):
    stocks = {}

    for date, values in data['Time Series (Daily)'].items():
        stocks[date] = values

        if len(stocks) == 90:
          break

    return stocks

@task
def load(data: dict, table_name: str, symbol: str, database: str = 'dev', schema: str = 'raw'):
    table = f'{database}.{schema}.{table_name}'

    cur = return_snowflake_conn()

    try:
        cur.execute("BEGIN;")

        cur.execute(f"CREATE DATABASE IF NOT EXISTS {database};")
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {database}.{schema};")

        # Create table
        cur.execute(
        f"""CREATE TABLE {table} IF NOT EXISTS (
          date DATE PRIMARY KEY,
          symbol VARCHAR(4),
          open NUMBER(9,4),
          close NUMBER(9,4),
          high NUMBER(9,4),
          low NUMBER(9,4),
          volume INT
        );"""
        )
        cur.execute(f"DELETE FROM {table};")

        # Insert data into table
        for date, numbers in data.items():
          open = numbers['1. open']
          high = numbers['2. high']
          low = numbers['3. low']
          close = numbers['4. close']
          volume = numbers['5. volume']

          statement = f'''INSERT INTO {table}
          (date, symbol, open, close, high, low, volume)
          VALUES (TO_DATE(\'{date}\'), \'{symbol}\', {open}, {close}, {high}, {low}, {volume});'''
          cur.execute(statement)

        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        print(e)
        raise e


with DAG(
    dag_id = 'HW5',
    start_date = datetime(2024,9,21),
    catchup=False,
    tags=['ETL'],
    schedule = '30 2 * * *'
) as dag:
    database = 'dev'
    schema = 'raw'
    table_name = 'snow_stock'

    symbol = "SNOW"

    data = extract(symbol)
    transformed = transform(data)
    load(transformed, table_name, symbol, database, schema)
