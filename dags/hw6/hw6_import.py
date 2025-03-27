# This file contains code that completes question 1 of homework 6 for DATA 226

from airflow.decorators import task
from airflow import DAG
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import datetime

def return_snowflake_conn():
    # Initialize the SnowflakeHook
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')

    # Execute the query and fetch results
    conn = hook.get_conn()
    return conn.cursor()

@task
def load_data(database, schema):
    print(f'{database}.{schema}')

    cur = return_snowflake_conn()

    try:
        # User session channel table creation
        usc_creation = f'''CREATE OR REPLACE TABLE {database}.{schema}.user_session_channel (
    userId int not NULL,
    sessionId varchar(32) primary key,
    channel varchar(32) default 'direct'
);'''
        print(usc_creation)
        cur.execute(usc_creation)

        # Session timestamp table creation
        sts_creation = f'''CREATE OR REPLACE TABLE {database}.{schema}.session_timestamp (
    sessionId varchar(32) primary key,
    ts timestamp
);'''
        print(sts_creation)
        cur.execute(sts_creation)

        # Creating stage
        stage_creation = f'''CREATE OR REPLACE STAGE {database}.{schema}.blob_stage
url = 's3://s3-geospatial/readonly/'
file_format = (type = csv, skip_header = 1, field_optionally_enclosed_by = '"');'''
        print(stage_creation)
        cur.execute(stage_creation)

        # Copy into the databases
        usc_copy = f'''COPY INTO {database}.{schema}.user_session_channel
FROM @{database}.{schema}.blob_stage/user_session_channel.csv;'''
        print(usc_copy)
        cur.execute(usc_copy)

        sts_copy = f'''COPY INTO {database}.{schema}.session_timestamp
FROM @{database}.{schema}.blob_stage/session_timestamp.csv;'''
        print(sts_copy)
        cur.execute(sts_copy)
    except Exception as e:
        raise e

with DAG(
    dag_id = 'Import_Tables',
    start_date = datetime(2024,10,2),
    catchup=False,
    tags=['ETL'],
    schedule = '45 2 * * *'
) as dag:
    database = "user_db_bullfrog"
    schema = "raw"

    load_data(database, schema)
