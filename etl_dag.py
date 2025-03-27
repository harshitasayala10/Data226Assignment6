
# In Cloud Composer, add apache-airflow-providers-snowflake to PYPI Packages
from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.operators.python import get_current_context
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

import snowflake.connector
import requests
from datetime import datetime, timedelta


def return_snowflake_conn():

    # Initialize the SnowflakeHook
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    
    # Execute the query and fetch results
    conn = hook.get_conn()
    return conn.cursor()

@task
def set_stage():
    cur = return_snowflake_conn()
    try:
        cur.execute("BEGIN;")
        cur.execute(f"""CREATE TABLE IF NOT EXISTS DEV.RAW.user_session_channel (userId int not NULL,
                    sessionId varchar(32) primary key,
                    channel varchar(32) default 'direct' ); """)
        cur.execute(f"""CREATE TABLE IF NOT EXISTS DEV.RAW.session_timestamp (sessionId varchar(32) primary key,
                    ts timestamp );""")
        cur.execute(f"""CREATE OR REPLACE STAGE DEV.RAW.blob_stage
                    url = 's3://s3-geospatial/readonly/'
                    file_format = (type = csv, skip_header = 1, field_optionally_enclosed_by = '"');""")
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        print(e)
        raise e

@task
def load():
    cur = return_snowflake_conn()
    try:
        cur.execute("BEGIN;")
        cur.execute(f"""COPY INTO DEV.RAW.user_session_channel
                    FROM @DEV.RAW.blob_stage/user_session_channel.csv""")
        cur.execute(f"""COPY INTO DEV.RAW.session_timestamp
                    FROM @DEV.RAW.blob_stage/session_timestamp.csv""")
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        print(e)
        raise e

with DAG(
    dag_id = 'ETL',
    start_date = datetime(2025, 3,26),
    catchup=False,
    tags=['ETL'],
    schedule = '30 2 * * *'
) as dag:
    user_session_table = "DEV.RAW.user_session_channel"
    session_timestamp_table = "DEV.RAW.session_timestamp"
    set_stage()
    load()
