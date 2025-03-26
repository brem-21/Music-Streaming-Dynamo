import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import datetime

# Initialize Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Get job parameters
try:
    args = getResolvedOptions(sys.argv, ['JOB_NAME', 'database_name'])
    database_name = args['database_name']
except:
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    database_name = 'music_streams' 

# Initialize job
job.init(args['JOB_NAME'], args)

print(f"Processing data from Glue database: {database_name}")

# Helper function to write to DynamoDB
def write_to_dynamodb(df, table_name, partition_key, sort_key=None):
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
    # Step 1: Read reference data (users and songs) from Glue Data Catalog
    print("Reading reference data from Glue Data Catalog...")
    
    # Read users table
    users_dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
        database=database_name, 
        table_name="users"
    )
    users_df = users_dynamic_frame.toDF()
    
    # Read songs table
    songs_dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
        database=database_name, 
        table_name="songs"
    )
    songs_df = songs_dynamic_frame.toDF()
    
    print(f"Users count: {users_df.count()}")
    print(f"Songs count: {songs_df.count()}")
    
    # Step 2: Read raw_data table (streams) from Glue Data Catalog
    print("Reading raw_data table from Glue Data Catalog...")
    raw_data_dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
        database=database_name, 
        table_name="raw_data"
    )
    raw_data_df = raw_data_dynamic_frame.toDF()
    
    # Step 3: Process each stream based on partition_0
    print("Processing streams based on partition_0...")
    
    # Get unique partitions (streams1, streams2, streams3)
    partitions = raw_data_df.select("partition_0").distinct().collect()
    
    for partition in partitions:
        partition_name = partition["partition_0"]
        print(f"Processing partition: {partition_name}")
        
        # Filter data for the current partition
        streams_df = raw_data_df.filter(F.col("partition_0") == partition_name)
        
        # Convert listen_time to timestamp if it's not already in timestamp format
        if "listen_time" in streams_df.columns:
            streams_df = streams_df.withColumn("listen_time", F.to_timestamp("listen_time"))
        
        # Validate streams data
        print(f"Stream {partition_name} count: {streams_df.count()}")
        
        # Check for required columns
        required_columns = ["user_id", "track_id", "listen_time"]
        missing_columns = [col for col in required_columns if col not in streams_df.columns]
        
        if missing_columns:
            print(f"WARNING: Missing required columns in partition {partition_name}: {missing_columns}")
            continue
        
        # Check for null values in required columns
        null_counts = streams_df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in required_columns])
        null_counts_collected = null_counts.collect()[0].asDict()
        
        for col, count in null_counts_collected.items():
            if count > 0:
                print(f"WARNING: Column {col} has {count} null values in partition {partition_name}")
        
        # Step 4: Enrich stream data with user and song information
        enriched_df = streams_df.join(users_df, "user_id", "left")
        enriched_df = enriched_df.join(songs_df, "track_id", "left")
        
        # Add date columns for partitioning and analysis
        enriched_df = enriched_df.withColumn("processing_date", F.current_date())
        enriched_df = enriched_df.withColumn("listen_date", F.to_date(F.col("listen_time")))
        
        # Convert duration_ms to seconds (handle potential string format)
        enriched_df = enriched_df.withColumn("duration_seconds", 
                                            F.when(F.col("duration_ms").isNotNull(), 
                                                  F.col("duration_ms").cast("double") / 1000)
                                            .otherwise(0))
        
        # Clean data - remove rows with missing key information
        cleaned_df = enriched_df.filter(F.col("user_id").isNotNull() & 
                                       F.col("track_id").isNotNull() & 
                                       F.col("listen_time").isNotNull())
        
        # Write the processed data partitioned by date
        print(f"Writing processed data for partition {partition_name}...")
        timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        processed_path = f"s3://music-streaming-data-dynamo/processed_data/{partition_name}_{timestamp}/"
        
        try:
            cleaned_df.write.mode("overwrite").partitionBy("listen_date").parquet(processed_path)
            print(f"Successfully wrote processed data to: {processed_path}")
        except Exception as e:
            print(f"Error writing to S3: {str(e)}")
            raise
        
        print(f"Completed processing partition {partition_name}")
        
        # Step 5: Compute KPIs and load into DynamoDB
        print(f"Computing KPIs for partition {partition_name}...")
        
        # 1. Genre Listen Count
        genre_listen_count_df = cleaned_df.groupBy("track_genre", "listen_date") \
            .agg(F.count("*").alias("listen_count"))
        
        # 2. Unique Listeners
        unique_listeners_df = cleaned_df.groupBy("track_genre", "listen_date") \
            .agg(F.countDistinct("user_id").alias("unique_listeners"))
        
        # 3. Total Listening Time
        total_listening_time_df = cleaned_df.groupBy("track_genre", "listen_date") \
            .agg(F.sum("duration_seconds").alias("total_listening_time"))
        
        # 4. Average Listening Time per User
        avg_listening_time_df = cleaned_df.groupBy("track_genre", "listen_date", "user_id") \
            .agg(F.sum("duration_seconds").alias("user_listening_time")) \
            .groupBy("track_genre", "listen_date") \
            .agg(F.avg("user_listening_time").alias("avg_listening_time_per_user"))
        
        # 5. Top 3 Songs per Genre per Day
        window_spec = Window.partitionBy("track_genre", "listen_date").orderBy(F.desc("song_listen_count"))
        top_songs_df = cleaned_df.groupBy("track_genre", "listen_date", "track_id", "track_name") \
            .agg(F.count("*").alias("song_listen_count")) \
            .withColumn("rank", F.row_number().over(window_spec)) \
            .filter(F.col("rank") <= 3) \
            .select("track_genre", "listen_date", "track_id", "track_name", "song_listen_count", "rank")
        
        # 6. Top 5 Genres per Day
        top_genres_df = genre_listen_count_df \
            .withColumn("rank", F.row_number().over(Window.partitionBy("listen_date").orderBy(F.desc("listen_count")))) \
            .filter(F.col("rank") <= 5) \
            .select("track_genre", "listen_date", "listen_count", "rank")
        
        # Remove duplicates before writing to DynamoDB
        genre_listen_count_df = genre_listen_count_df.dropDuplicates(["track_genre", "listen_date"])
        unique_listeners_df = unique_listeners_df.dropDuplicates(["track_genre", "listen_date"])
        total_listening_time_df = total_listening_time_df.dropDuplicates(["track_genre", "listen_date"])
        avg_listening_time_df = avg_listening_time_df.dropDuplicates(["track_genre", "listen_date"])
        top_songs_df = top_songs_df.dropDuplicates(["track_genre", "track_id"])
        top_genres_df = top_genres_df.dropDuplicates(["listen_date"])
        
        # Write KPIs to DynamoDB
        write_to_dynamodb(genre_listen_count_df, "music_streaming_genre_listen_count", "track_genre", "listen_date")
        write_to_dynamodb(unique_listeners_df, "music_streaming_unique_listeners", "track_genre", "listen_date")
        write_to_dynamodb(total_listening_time_df, "music_streaming_total_listening_time", "track_genre", "listen_date")
        write_to_dynamodb(avg_listening_time_df, "music_streaming_avg_listening_time", "track_genre", "listen_date")
        write_to_dynamodb(top_songs_df, "music_streaming_top_songs", "track_genre", "track_id")
        write_to_dynamodb(top_genres_df, "music_streaming_top_genres", "listen_date")
        
        print(f"Successfully loaded KPIs into DynamoDB for partition {partition_name}")
    
    print("ETL process completed successfully")
    
except Exception as e:
    print(f"Error in ETL process: {str(e)}")
    raise

job.commit()