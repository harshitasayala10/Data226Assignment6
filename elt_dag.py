from airflow.decorators import task
from airflow import DAG
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
import logging

def get_snowflake_cursor():
    """Helper function to get Snowflake cursor"""
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = hook.get_conn()
    return conn.cursor()

@task
def create_session_summary():
    """Creates analytics.session_summary by joining raw tables and checks for duplicates."""
    cur = get_snowflake_cursor()
    try:
        cur.execute("BEGIN;")
        
        # CTAS to create the joined table
        sql = """
        CREATE OR REPLACE TABLE DEV.ANALYTICS.SESSION_SUMMARY AS
        SELECT 
            u.userId,
            u.sessionId,
            u.channel,
            s.ts
        FROM DEV.RAW.USER_SESSION_CHANNEL u
        JOIN DEV.RAW.SESSION_TIMESTAMP s 
            ON u.sessionId = s.sessionId
        """
        logging.info("Creating session_summary table...")
        cur.execute(sql)

        # Check for duplicates (extra +1pt requirement)
        logging.info("Checking for duplicates...")
        cur.execute("SELECT COUNT(1) FROM DEV.ANALYTICS.SESSION_SUMMARY")
        total_count = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(1) FROM (SELECT DISTINCT * FROM DEV.ANALYTICS.SESSION_SUMMARY)")
        distinct_count = cur.fetchone()[0]
        
        if total_count != distinct_count:
            raise Exception(f"Duplicate records found! Total={total_count}, Distinct={distinct_count}")
        
        cur.execute("COMMIT;")
        logging.info("Successfully created session_summary with no duplicates.")
    
    except Exception as e:
        cur.execute("ROLLBACK;")
        logging.error("Failed to create session_summary. Rolled back changes.")
        raise

with DAG(
    dag_id="ELT_SESSION_SUMMARY",
    start_date=datetime(2024, 10, 2),
    schedule_interval="45 2 * * *",  # Runs after raw tables are loaded (02:45 UTC)
    catchup=False,
    tags=["elt", "snowflake"],
) as dag:
    create_session_summary()
