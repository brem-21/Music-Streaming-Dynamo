from airflow import DAG
from airflow.providers.amazon.aws.hooks.glue import AwsGlueJobHook
from airflow.operators.python import PythonOperator
from datetime import datetime


def test_glue_connection():
    glue_hook = AwsGlueJobHook(aws_conn_id="aws_conn")
    try:
        # List Glue jobs to test the connection
        client = glue_hook.get_client_type("glue")
        response = client.get_jobs()
        print("Successfully connected to AWS Glue. Job count:", len(response["Jobs"]))
    except Exception as e:
        print("Glue connection test failed:", str(e))
        raise
    
# Define the DAG
with DAG(
    dag_id="test_glue_connection",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    test_connection = PythonOperator(
        task_id="test_glue",
        python_callable=test_glue_connection
    )
    test_connection