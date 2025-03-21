import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as F

# Initialize Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Get job parameters
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'bucket_name', 'stream_idx'])
job.init(args['JOB_NAME'], args)

# Define bucket name and stream index
bucket_name = args['bucket_name']
stream_idx = args['stream_idx']

# Define paths
processed_data_path = f"s3://{bucket_name}/processed_data/dynamodb/stream{stream_idx}/"

print(f"Loading data from {processed_data_path} to DynamoDB for stream {stream_idx}")

# Helper function to write to DynamoDB
def write_to_dynamodb(df, table_name):
    try:
        # Convert DataFrame to DynamicFrame
        dynamic_frame = DynamicFrame.fromDF(df, glueContext, table_name)
        
        # Write to DynamoDB
        glueContext.write_dynamic_frame.from_options(
            frame=dynamic_frame,
            connection_type="dynamodb",
            connection_options={
                "dynamodb.output.tableName": table_name,
                "dynamodb.throughput.write.percent": "1.0"
            }
        )
        print(f"Successfully wrote to DynamoDB table: {table_name}")
    except Exception as e:
        print(f"Error writing to DynamoDB table {table_name}: {str(e)}")
        raise

try:
    # 1. Load Genre Listen Count
    print("Loading Genre Listen Count...")
    genre_listen_count_df = spark.read.parquet(processed_data_path + "genre_listen_count/")
    write_to_dynamodb(genre_listen_count_df, "music_streaming_genre_listen_count")
    
    # 2. Load Unique Listeners
    print("Loading Unique Listeners...")
    unique_listeners_df = spark.read.parquet(processed_data_path + "unique_listeners/")
    write_to_dynamodb(unique_listeners_df, "music_streaming_unique_listeners")
    
    # 3. Load Total Listening Time
    print("Loading Total Listening Time...")
    total_listening_time_df = spark.read.parquet(processed_data_path + "total_listening_time/")
    write_to_dynamodb(total_listening_time_df, "music_streaming_total_listening_time")
    
    # 4. Load Average Listening Time
    print("Loading Average Listening Time...")
    avg_listening_time_df = spark.read.parquet(processed_data_path + "avg_listening_time/")
    write_to_dynamodb(avg_listening_time_df, "music_streaming_avg_listening_time")
    
    # 5. Load Top Songs by Genre
    print("Loading Top Songs...")
    top_songs_df = spark.read.parquet(processed_data_path + "top_songs/")
    write_to_dynamodb(top_songs_df, "music_streaming_top_songs")
    
    # 6. Load Top Genres
    print("Loading Top Genres...")
    top_genres_df = spark.read.parquet(processed_data_path + "top_genres/")
    write_to_dynamodb(top_genres_df, "music_streaming_top_genres")
    
    print(f"Successfully loaded all data to DynamoDB for stream {stream_idx}")
    
except Exception as e:
    print(f"Error loading data to DynamoDB: {str(e)}")
    raise

job.commit()
