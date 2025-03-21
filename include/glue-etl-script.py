import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *
import datetime

# Initialize Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Get job parameters
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'bucket_name'])
job.init(args['JOB_NAME'], args)

# Define bucket name
bucket_name = args['bucket_name']

# Define paths
raw_data_path = f"s3://{bucket_name}/raw_data/"
processed_data_path = f"s3://{bucket_name}/processed_data/"
reference_data_path = f"s3://{bucket_name}/reference_data/"
kpi_data_path = f"s3://{bucket_name}/kpi_data/"

print(f"Processing data from bucket: {bucket_name}")

# Define schema based on the provided schema information
users_schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("user_name", StringType(), True),
    StructField("user_age", IntegerType(), True),
    StructField("user_country", StringType(), True),
    StructField("created_at", DateType(), True)
])

songs_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("track_id", StringType(), True),
    StructField("artists", StringType(), True),
    StructField("album_name", StringType(), True),
    StructField("track_name", StringType(), True),
    StructField("popularity", StringType(), True),
    StructField("duration_ms", StringType(), True),
    StructField("explicit", StringType(), True),
    StructField("danceability", StringType(), True),
    StructField("energy", StringType(), True),
    StructField("key", StringType(), True),
    StructField("loudness", StringType(), True),
    StructField("mode", StringType(), True),
    StructField("speechiness", StringType(), True),
    StructField("acousticness", StringType(), True),
    StructField("instrumentalness", DoubleType(), True),
    StructField("liveness", StringType(), True),
    StructField("valence", StringType(), True),
    StructField("tempo", DoubleType(), True),
    StructField("time_signature", DoubleType(), True),
    StructField("track_genre", StringType(), True)
])

streams_schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("track_id", StringType(), True),
    StructField("listen_time", TimestampType(), True)
])

try:
    # Step 1: Read reference data (users and songs) from CSV files
    print("Reading reference data...")
    users_df = spark.read.csv(f"{reference_data_path}users/users.csv", header=True, schema=users_schema)
    songs_df = spark.read.csv(f"{reference_data_path}songs/songs.csv", header=True, schema=songs_schema)
    
    print(f"Users count: {users_df.count()}")
    print(f"Songs count: {songs_df.count()}")
    
    # Step 2: Process each stream file
    for stream_idx in range(1, 4):
        stream_path = f"{raw_data_path}stream{stream_idx}/streams{stream_idx}.csv"
        print(f"Processing stream {stream_idx} from {stream_path}")
        
        # Read stream data
        streams_df = spark.read.csv(stream_path, header=True)
        
        # Convert listen_time to timestamp if it's not already in timestamp format
        if "listen_time" in streams_df.columns:
            streams_df = streams_df.withColumn("listen_time", F.to_timestamp("listen_time"))
        
        # Validate streams data
        print(f"Stream {stream_idx} count: {streams_df.count()}")
        
        # Check for required columns
        required_columns = ["user_id", "track_id", "listen_time"]
        missing_columns = [col for col in required_columns if col not in streams_df.columns]
        
        if missing_columns:
            print(f"WARNING: Missing required columns in stream {stream_idx}: {missing_columns}")
            continue
        
        # Check for null values in required columns
        null_counts = streams_df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in required_columns])
        null_counts_collected = null_counts.collect()[0].asDict()
        
        for col, count in null_counts_collected.items():
            if count > 0:
                print(f"WARNING: Column {col} has {count} null values in stream {stream_idx}")
        
        # Step 3: Enrich stream data with user and song information
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
        print(f"Writing processed data for stream {stream_idx}...")
        processed_path = f"{processed_data_path}stream{stream_idx}/"
        cleaned_df.write.mode("overwrite").partitionBy("listen_date").parquet(processed_path)
        
        # Step 4: Compute KPIs
        print(f"Computing KPIs for stream {stream_idx}...")
        
        # Convert to a standard date format for grouping
        df = cleaned_df.withColumn("date", F.to_date("listen_date"))
        
        # Create windows for ranking
        genre_window = Window.partitionBy("date", "track_genre").orderBy(F.desc("listen_count"))
        date_window = Window.partitionBy("date").orderBy(F.desc("listen_count"))
        
        # 1. Listen Count by genre per day
        genre_listen_count = df.groupBy("date", "track_genre") \
            .agg(F.count("*").alias("listen_count")) \
            .withColumn("rank", F.row_number().over(date_window))
        
        # 2. Top 5 genres by day
        top_genres = genre_listen_count.filter(F.col("rank") <= 5) \
            .select("date", "track_genre", "listen_count", "rank") \
            .withColumn("metric_type", F.lit("top_genres"))
        
        # 3. Unique Listeners by genre per day
        unique_listeners = df.groupBy("date", "track_genre") \
            .agg(F.countDistinct("user_id").alias("unique_listeners")) \
            .withColumn("metric_type", F.lit("unique_listeners"))
        
        # 4. Total Listening Time by genre per day
        total_listening_time = df.groupBy("date", "track_genre") \
            .agg(F.sum("duration_seconds").alias("total_listening_seconds")) \
            .withColumn("metric_type", F.lit("total_listening_time"))
        
        # 5. Average Listening Time per User by genre per day
        avg_listening_time = df.groupBy("date", "track_genre", "user_id") \
            .agg(F.sum("duration_seconds").alias("user_listening_time")) \
            .groupBy("date", "track_genre") \
            .agg(F.avg("user_listening_time").alias("avg_listening_time_per_user")) \
            .withColumn("metric_type", F.lit("avg_listening_time"))
        
        # 6. Top 3 Songs per Genre per Day
        song_listen_count = df.groupBy("date", "track_genre", "track_id", "track_name") \
            .agg(F.count("*").alias("song_listen_count")) \
            .withColumn("song_rank", F.row_number().over(genre_window))
        
        top_songs = song_listen_count.filter(F.col("song_rank") <= 3) \
            .select("date", "track_genre", "track_id", "track_name", "song_listen_count", "song_rank") \
            .withColumn("metric_type", F.lit("top_songs"))
        
        # Save KPIs to separate outputs
        stream_kpi_path = f"{kpi_data_path}stream{stream_idx}/"
        
        print(f"Writing KPIs for stream {stream_idx}...")
        genre_listen_count.write.mode("overwrite").parquet(stream_kpi_path + "genre_listen_count/")
        unique_listeners.write.mode("overwrite").parquet(stream_kpi_path + "unique_listeners/")
        total_listening_time.write.mode("overwrite").parquet(stream_kpi_path + "total_listening_time/")
        avg_listening_time.write.mode("overwrite").parquet(stream_kpi_path + "avg_listening_time/")
        top_songs.write.mode("overwrite").parquet(stream_kpi_path + "top_songs/")
        top_genres.write.mode("overwrite").parquet(stream_kpi_path + "top_genres/")
        
        # Step 5: Prepare data for DynamoDB
        print(f"Preparing DynamoDB data for stream {stream_idx}...")
        
        # Create composite keys for DynamoDB
        genre_listen_count_ddb = genre_listen_count.withColumn(
            "genre_date_key", 
            F.concat(F.col("track_genre"), F.lit("#"), F.date_format(F.col("date"), "yyyy-MM-dd"))
        )
        
        unique_listeners_ddb = unique_listeners.withColumn(
            "genre_date_key", 
            F.concat(F.col("track_genre"), F.lit("#"), F.date_format(F.col("date"), "yyyy-MM-dd"))
        )
        
        total_listening_time_ddb = total_listening_time.withColumn(
            "genre_date_key", 
            F.concat(F.col("track_genre"), F.lit("#"), F.date_format(F.col("date"), "yyyy-MM-dd"))
        )
        
        avg_listening_time_ddb = avg_listening_time.withColumn(
            "genre_date_key", 
            F.concat(F.col("track_genre"), F.lit("#"), F.date_format(F.col("date"), "yyyy-MM-dd"))
        )
        
        top_songs_ddb = top_songs.withColumn(
            "genre_date_song_key", 
            F.concat(F.col("track_genre"), F.lit("#"), F.date_format(F.col("date"), "yyyy-MM-dd"), F.lit("#"), F.col("track_id"))
        )
        
        top_genres_ddb = top_genres.withColumn(
            "date_rank_key", 
            F.concat(F.date_format(F.col("date"), "yyyy-MM-dd"), F.lit("#"), F.col("rank"))
        )
        
        # Save DynamoDB formatted data
        ddb_path = f"{processed_data_path}dynamodb/stream{stream_idx}/"
        
        print(f"Writing DynamoDB formatted data for stream {stream_idx}...")
        genre_listen_count_ddb.write.mode("overwrite").parquet(ddb_path + "genre_listen_count/")
        unique_listeners_ddb.write.mode("overwrite").parquet(ddb_path + "unique_listeners/")
        total_listening_time_ddb.write.mode("overwrite").parquet(ddb_path + "total_listening_time/")
        avg_listening_time_ddb.write.mode("overwrite").parquet(ddb_path + "avg_listening_time/")
        top_songs_ddb.write.mode("overwrite").parquet(ddb_path + "top_songs/")
        top_genres_ddb.write.mode("overwrite").parquet(ddb_path + "top_genres/")
        
        print(f"Completed processing stream {stream_idx}")
    
    print("ETL process completed successfully")
    
except Exception as e:
    print(f"Error in ETL process: {str(e)}")
    raise

job.commit()
