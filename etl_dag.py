from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def get_snowflake_cursor():
    """Helper function to get Snowflake cursor"""
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = hook.get_conn()
    return conn.cursor()

@task
def create_tables_and_stage():
    """Task to create tables and stage in Snowflake"""
    cur = get_snowflake_cursor()
    try:
        # Using multi-statement execution for atomic operations
        cur.execute("""
            BEGIN;
            CREATE SCHEMA IF NOT EXISTS DEV.RAW;
            
            CREATE TABLE IF NOT EXISTS DEV.RAW.user_session_channel (
                userId int not NULL,
                sessionId varchar(32) primary key,
                channel varchar(32) default 'direct'
            );
            
            CREATE TABLE IF NOT EXISTS DEV.RAW.session_timestamp (
                sessionId varchar(32) primary key,
                ts timestamp
            );
            
            CREATE OR REPLACE STAGE DEV.RAW.blob_stage
            url = 's3://s3-geospatial/readonly/'
            file_format = (type = csv, skip_header = 1, field_optionally_enclosed_by = '"');
            COMMIT;
        """)
    except Exception as e:
        cur.execute("ROLLBACK;")
        raise e
    finally:
        cur.close()

@task
def load_data():
    """Task to load data from S3 to Snowflake tables"""
    cur = get_snowflake_cursor()
    try:
        cur.execute("""
            BEGIN;
            COPY INTO DEV.RAW.user_session_channel
            FROM @DEV.RAW.blob_stage/user_session_channel.csv;
            
            COPY INTO DEV.RAW.session_timestamp
            FROM @DEV.RAW.blob_stage/session_timestamp.csv;
            COMMIT;
        """)
    except Exception as e:
        cur.execute("ROLLBACK;")
        raise e
    finally:
        cur.close()

with DAG(
    dag_id='snowflake_etl_dag',
    default_args=default_args,
    description='ETL DAG to load data from S3 to Snowflake',
    schedule_interval='30 2 * * *',  # Daily at 02:30 UTC
    start_date=datetime(2025, 3, 26),
    catchup=False,
    tags=['etl', 'snowflake'],
) as dag:
    
    # Define task dependencies
    create_infrastructure = create_tables_and_stage()
    load_data_task = load_data()
    
    # Set task order
    create_infrastructure >> load_data_task
