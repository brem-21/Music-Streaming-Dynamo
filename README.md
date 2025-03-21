# Music Streaming Analytics Pipeline

## Overview
A robust ETL pipeline built with Apache Airflow that processes music streaming data and stores analytics in DynamoDB. The pipeline aggregates listening patterns, calculates key metrics, and provides insights into music consumption trends.

## Architecture

### Data Flow
1. **Source Data** (S3)
   - User data
   - Song metadata
   - Streaming events

2. **Processing Layer** (AWS Glue)
   - Data validation
   - Schema conformity checks
   - ETL transformations

3. **Storage Layer** (DynamoDB)
   - Genre-based listening metrics
   - User engagement statistics
   - Temporal analytics

### DynamoDB Tables
- `music_streaming_genre_listen_count`: Track genre popularity
- `music_streaming_unique_listeners`: Unique listener counts
- `music_streaming_total_listening_time`: Cumulative listening duration
- `music_streaming_avg_listening_time`: Average session duration
- `music_streaming_top_songs`: Most popular tracks
- `music_streaming_top_genres`: Trending genres

## Prerequisites

### Local Development
```bash
python 3.10+
Apache Airflow 2.7.1+
AWS CLI
```

### AWS Services
- S3 bucket for data storage
- DynamoDB tables
- AWS Glue for ETL jobs
- IAM roles with appropriate permissions

## Quick Start

1. **Clone and Setup**
```bash
git clone <repository-url>
cd Music-Streaming-Dynamo
make install
```

2. **Configure AWS Resources**
```bash
# Set up required AWS resources
./setup-script.sh
```

3. **Set Airflow Variables**
```python
bucket_name: "music-streaming-data-dynamo"
glue_job_name: "<your-glue-job-name>"
glue_script_location: "s3://<bucket>/glue_scripts/"
database_name: "music_streams"
```

4. **Run the Pipeline**
```bash
# Using Astro CLI
astro dev start
```

## Development

### Project Structure
```
Music-Streaming-Dynamo/
├── dags/
│   └── music_streaming_with_etl.py
├── include/
│   ├── glue-etl-script.py
│   └── dynamodb-loader-script.py
├── setup-script.sh
├── requirements.txt
└── Dockerfile
```

### Make Commands
```bash
make install    # Install dependencies
make format     # Format code with black
make lint       # Run pylint
make refactor   # Run format and lint
make all        # Run all commands
```

## Data Schema

### Input Data
- **Users**: user_id, user_name, user_age, user_country, created_at
- **Songs**: track_id, artists, album_name, track_name, popularity, duration_ms, track_genre
- **Streams**: user_id, track_id, listen_time

### Processed Metrics
- Genre-based listening patterns
- User engagement metrics
- Temporal trends
- Popular track analytics

## Monitoring

### Airflow UI
- Task execution status
- DAG run history
- Error logs
- Performance metrics

### AWS CloudWatch
- Glue job metrics
- DynamoDB throughput
- S3 operations

## Contributing
1. Fork the repository
2. Create a feature branch
3. Make changes and test
4. Run code quality checks:
```bash
make refactor
```
5. Submit a pull request

## Docker Support
The project includes Docker support using Astro Runtime:
```bash
# Build and run using Astro
astro dev start
```
