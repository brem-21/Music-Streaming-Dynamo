from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python_operator import PythonOperator
import boto3
from airflow.hooks.base_hook import BaseHook
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
from airflow.models import Variable
from datetime import datetime, timedelta
import pandas as pd
import io

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 20),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,  # Increase the number of retries
    'retry_delay': timedelta(minutes=5),  # Delay between retries
}

# Define the DAG
dag = DAG(
    'music_streaming_pipeline_dynamo',
    default_args=default_args,
    description='Music Streaming Data Pipeline',
    schedule_interval=None,  # Run when triggered
    catchup=False,
)

# Retrieve Airflow Variables
bucket_name = Variable.get("bucket_name")  # Retrieve bucket_name
glue_job_name = Variable.get("glue_job_name")  # Retrieve glue_job_name
glue_script_location = Variable.get("glue_script_location")  # Retrieve glue_script_location
database_name = Variable.get("database_name", default_var="music_streams")  # Retrieve database_name

# Check if files exist in S3 using S3Hook
def check_file_exists(bucket, key):
    hook = S3Hook(aws_conn_id='aws_conn')
    return hook.check_for_key(key=key, bucket_name=bucket)

# Validate data files before processing
def validate_data_files(**kwargs):
    bucket = kwargs['bucket']
    
    # Check for reference data
    users_path = "reference_data/users/users.csv"
    if not check_file_exists(bucket, users_path):
        print(f"File not found: s3://{bucket}/{users_path}")
        raise ValueError("Users reference data not found!")
    
    songs_path = "reference_data/songs/songs.csv"
    if not check_file_exists(bucket, songs_path):
        print(f"File not found: s3://{bucket}/{songs_path}")
        raise ValueError("Songs reference data not found!")
    
    # Check for stream files
    for stream_idx in range(1, 4):
        stream_key = f"raw_data/streams{stream_idx}/streams{stream_idx}.csv"  # Updated path
        if not check_file_exists(bucket, stream_key):
            print(f"File not found: s3://{bucket}/{stream_key}")
            raise ValueError(f"Stream data {stream_key} not found!")
    
    print("All required data files found.")
    return True

def move_s3_files(source_bucket, source_prefix, dest_bucket, dest_prefix):
    
    conn = BaseHook.get_connection("aws_conn")
    s3_client = boto3.client("s3",
                         aws_access_key_id=conn.login,
                         aws_secret_access_key=conn.password,
                         region_name="eu-west-1")
    try:
        # List objects in the source bucket
        response = s3_client.list_objects_v2(Bucket=source_bucket, Prefix=source_prefix)
        if 'Contents' not in response:
            print(f"No files found in s3://{source_bucket}/{source_prefix}")
            return

        # Move each file to the destination bucket
        for obj in response['Contents']:
            source_key = obj['Key']
            dest_key = source_key.replace(source_prefix, dest_prefix, 1)
            copy_source = {'Bucket': source_bucket, 'Key': source_key}
            s3_client.copy_object(CopySource=copy_source, Bucket=dest_bucket, Key=dest_key)
            s3_client.delete_object(Bucket=source_bucket, Key=source_key)
            print(f"Moved s3://{source_bucket}/{source_key} to s3://{dest_bucket}/{dest_key}")

        print("All files moved successfully.")
    except (NoCredentialsError, PartialCredentialsError) as e:
        print(f"Credentials error: {str(e)}")
        raise
    except Exception as e:
        print(f"Error moving files: {str(e)}")
        raise

# Validate columns in the data files
def validate_columns(**kwargs):
    bucket = kwargs['bucket']
    hook = S3Hook(aws_conn_id='aws_conn')
    
    # Define required columns for each file
    required_columns = {
        "reference_data/users/users.csv": ["user_id", "user_name", "user_age", "user_country", "created_at"],
        "reference_data/songs/songs.csv": ["id", "track_id", "artists", "album_name", "track_name", "popularity", "duration_ms", "explicit", "danceability", "energy", "key", "loudness", "mode", "speechiness", "acousticness", "instrumentalness", "liveness", "valence", "tempo", "time_signature", "track_genre"],
        "raw_data/streams1/streams1.csv": ["user_id", "track_id", "listen_time"],  # Updated path
        "raw_data/streams2/streams2.csv": ["user_id", "track_id", "listen_time"],  # Updated path
        "raw_data/streams3/streams3.csv": ["user_id", "track_id", "listen_time"],  # Updated path
    }

    print(f"bucket_name : {bucket_name}")
    print(f"glue: {glue_job_name}")
    print(glue_script_location)
    print(database_name)
    
    for file_key, columns in required_columns.items():
        if not check_file_exists(bucket, file_key):
            print(f"File not found: s3://{bucket}/{file_key}")
            raise ValueError(f"File {file_key} not found!")
        
        # Read the file from S3
        obj = hook.get_key(key=file_key, bucket_name=bucket)
        df = pd.read_csv(io.BytesIO(obj.get()['Body'].read()))
        
        # Check for required columns
        missing_columns = [col for col in columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"File {file_key} is missing required columns: {missing_columns}")
        
        print(f"File {file_key} has all required columns.")
    
    print("All files have the required columns.")
    return True

# Create validate data task
validate_data = PythonOperator(
    task_id='validate_data_files',
    python_callable=validate_data_files,
    op_kwargs={'bucket': bucket_name},
    dag=dag,
)

# Create validate columns task
validate_columns = PythonOperator(
    task_id='validate_columns',
    python_callable=validate_columns,
    op_kwargs={'bucket': bucket_name},
    dag=dag,
)

# Create Glue ETL job
etl_job = GlueJobOperator(
    task_id='run_etl_job',
    job_name=glue_job_name,  # Name of the Glue job
    script_location=glue_script_location,  # S3 path to the Glue script
    aws_conn_id='aws_conn',  # Airflow connection ID for AWS
    region_name='eu-west-1',  # AWS region
    script_args={
       '--database_name': database_name,  # Pass the database name as an argument
    },
    dag=dag,
)


# Create archive task
archive_task = PythonOperator(
    task_id='archive_raw_data',
    python_callable=move_s3_files,
    op_kwargs={
        'source_bucket': bucket_name,
        'source_prefix': 'raw_data/',
        'dest_bucket': bucket_name,
        'dest_prefix': 'archived_data/',
    },
    dag=dag,
)

# Set task dependencies
validate_data >> validate_columns >> etl_job >> archive_task