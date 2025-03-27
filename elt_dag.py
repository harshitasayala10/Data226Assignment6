from airflow.decorators import task
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import get_current_context
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import datetime
from datetime import timedelta
import logging
import snowflake.connector

"""
This pipeline assumes that there are two other tables in your snowflake DB
 - user_session_channel
 - session_timestamp

With regard to how to set up these two tables, please refer to this README file:
 - https://github.com/keeyong/sjsu-data226/blob/main/week9/How-to-setup-ETL-tables-for-ELT.md
"""

def return_snowflake_conn():

    # Initialize the SnowflakeHook
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn_1')

    # Execute the query and fetch results
    conn = hook.get_conn()
    return conn.cursor()

@task
def run_ctas(table, select_sql, primary_key=None):

    logging.info(table)
    logging.info(select_sql)

    cur = return_snowflake_conn()

    try:
        cur.execute("BEGIN;")
        
        # Create or replace table using CTAS (Create Table As Select)
        sql = f"CREATE OR REPLACE TABLE {table} AS {select_sql}"
        logging.info(sql)
        cur.execute(sql)

        # Primary key uniqueness check
        if primary_key is not None:
            sql = f"SELECT {primary_key}, COUNT(1) AS cnt FROM {table} GROUP BY 1 ORDER BY 2 DESC LIMIT 1"
            print(sql)
            cur.execute(sql)
            result = cur.fetchone()
            print(result, result[1])
            if int(result[1]) > 1:
                print("!!!!!!!!!!!!!!")
                raise Exception(f"Primary key uniqueness failed: {result}")
        
        # Duplicate check
        logging.info("Running duplicate check on analytics.session_summary")
        
        # Query for total row count
        total_count_sql = "SELECT COUNT(1) FROM analytics.session_summary"
        print(total_count_sql)
        cur.execute(total_count_sql)
        total_count = cur.fetchone()[0]

        # Query for distinct row count
        distinct_count_sql = """
            SELECT COUNT(1) FROM (
                SELECT DISTINCT * FROM analytics.session_summary
            )
        """
        print(distinct_count_sql)
        cur.execute(distinct_count_sql)
        distinct_count = cur.fetchone()[0]

        # Check if duplicates exist
        if total_count != distinct_count:
            raise Exception(f"Duplicate records found in {table}: Total={total_count}, Distinct={distinct_count}")

        cur.execute("COMMIT;")
    
    except Exception as e:
        cur.execute("ROLLBACK")
        logging.error('Failed to execute SQL. Completed ROLLBACK!')
        logging.error(e)
        raise


with DAG(
    dag_id = 'ELT_CTAS',
    start_date = datetime(2024,10,2),
    catchup=False,
    tags=['ELT'],
    schedule = '45 2 * * *'
) as dag:

    table = "dev.analytics.session_summary"
    select_sql = """SELECT u.*, s.ts
    FROM DEV.RAW.user_session_channel u
    JOIN DEV.RAW.session_timestamp s ON u.sessionId=s.sessionId
    """

    run_ctas(table, select_sql, primary_key='sessionId')