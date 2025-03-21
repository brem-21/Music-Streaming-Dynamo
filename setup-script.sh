#!/bin/bash
# Script to set up AWS resources for the music streaming pipeline

# Set variables
BUCKET_NAME="music-streaming-data-dynamo"
REGION="eu-west-1" 

echo "Creating DynamoDB tables..."

# Create DynamoDB tables
aws dynamodb create-table \
    --table-name music_streaming_genre_listen_count \
    --attribute-definitions \
        AttributeName=genre_date_key,AttributeType=S \
    --key-schema \
        AttributeName=genre_date_key,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST \
    --region $REGION

aws dynamodb create-table \
    --table-name music_streaming_unique_listeners \
    --attribute-definitions \
        AttributeName=genre_date_key,AttributeType=S \
    --key-schema \
        AttributeName=genre_date_key,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST \
    --region $REGION

aws dynamodb create-table \
    --table-name music_streaming_total_listening_time \
    --attribute-definitions \
        AttributeName=genre_date_key,AttributeType=S \
    --key-schema \
        AttributeName=genre_date_key,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST \
    --region $REGION

aws dynamodb create-table \
    --table-name music_streaming_avg_listening_time \
    --attribute-definitions \
        AttributeName=genre_date_key,AttributeType=S \
    --key-schema \
        AttributeName=genre_date_key,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST \
    --region $REGION

aws dynamodb create-table \
    --table-name music_streaming_top_songs \
    --attribute-definitions \
        AttributeName=genre_date_song_key,AttributeType=S \
    --key-schema \
        AttributeName=genre_date_song_key,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST \
    --region $REGION

aws dynamodb create-table \
    --table-name music_streaming_top_genres \
    --attribute-definitions \
        AttributeName=date_rank_key,AttributeType=S \
    --key-schema \
        AttributeName=date_rank_key,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST \
    --region $REGION

echo "DynamoDB tables created successfully."

echo "Creating IAM role for Glue..."

# Create IAM role for Glue
aws iam create-role \
    --role-name AWSGlueServiceRole-MusicStreaming \
    --assume-role-policy-document '{
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {
                    "Service": "glue.amazonaws.com"
                },
                "Action": "sts:AssumeRole"
            }
        ]
    }'

# Attach policies to the role
aws iam attach-role-policy \
    --role-name AWSGlueServiceRole-MusicStreaming \
    --policy-arn arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole

aws iam attach-role-policy \
    --role-name AWSGlueServiceRole-MusicStreaming \
    --policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess

aws iam attach-role-policy \
    --role-name AWSGlueServiceRole-MusicStreaming \
    --policy-arn arn:aws:iam::aws:policy/AmazonDynamoDBFullAccess

echo "IAM role created and policies attached."

echo "Uploading Glue scripts to S3..."

# Upload Glue scripts to S3
aws s3 cp /include/glue-etl-script.py s3://$BUCKET_NAME/glue_scripts/
aws s3 cp /include/dynamodb-loader-script.py s3://$BUCKET_NAME/glue_scripts/

echo "Glue scripts uploaded."

echo "Setup completed successfully."
