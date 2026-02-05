import argparse
import logging
from pyspark.sql import SparkSession
from capstonellm.common.spark import ClosableSparkSession
import json
from pyspark.sql.functions import col, explode

logger = logging.getLogger(__name__)

def write_to_s3_partition(partition_iterator, bucket_name, base_path):
    """
    This function runs on the EXECUTORS.
    It takes a chunk of data and writes each row as a separate file to S3.
    """
    import boto3
    
    # Initialize S3 client on the worker
    s3_client = boto3.client("s3")
    
    for row in partition_iterator:
        record = row.asDict()
        # Use question_id for the filename to ensure uniqueness and logical naming
        q_id = record.get("question_id", "unknown")
        file_key = f"{base_path}/qa_pair_{q_id}.json"
        
        # Convert dict to pretty-printed JSON string
        json_data = json.dumps(record, indent=4)
        
        # Upload directly to S3
        s3_client.put_object(
            Bucket=bucket_name,
            Key=file_key,
            Body=json_data,
            ContentType="application/json"
        )

def clean(spark: SparkSession, env: str, tag: str, user: str = "Jaime"):
    logger.info(f"Starting job. Env: {env} | Tag: {tag} | User: {user}")

    bucket = "dataminded-academy-capstone-llm-data-us"
    questions_path = f"s3a://{bucket}/input/{tag}/questions.json"
    answers_path = f"s3a://{bucket}/input/{tag}/answers.json"
    
    # Base path for the individual JSON files (stripping s3a:// for boto3)
    s3_output_prefix = f"cleaned/{user}/{tag}"

    try:
        logger.info(f"Reading data from {tag}")
        df_q_raw = spark.read.option("multiLine", True).json(questions_path)
        df_a_raw = spark.read.option("multiLine", True).json(answers_path)

        # Transformation
        df_q = df_q_raw.select(explode(col("items")).alias("item")).select(
            col("item.question_id").alias("question_id"),
            col("item.title").alias("title"),
            col("item.body").alias("question_body")
        )

        df_a = df_a_raw.select(explode(col("items")).alias("item")).select(
            col("item.question_id").alias("question_id"),
            col("item.body").alias("answer_body")
        )

        # Join and select
        final_df = df_q.join(df_a, on="question_id", how="inner") \
                       .select("question_id", "title", "question_body", "answer_body")

        # --- Distributed Write via foreachPartition --- scalable lets go (the whole point of spakr right?)
        logger.info(f"Executors starting direct upload to s3://{bucket}/{s3_output_prefix}")
        
        # We pass the bucket and prefix to the workers
        final_df.foreachPartition(lambda it: write_to_s3_partition(it, bucket, s3_output_prefix))
        
        logger.info("Job finished successfully.")

    except Exception as e:
        logger.error(f"Critical error: {e}")
        raise e

def main():
    parser = argparse.ArgumentParser(description="capstone_llm")
    parser.add_argument(
        "-e", "--env", dest="env", help="environment we are executing in", required=False, default="local"
    )
    parser.add_argument(
        "-t", "--tag", dest="tag", help="the tag to process",
        default="python-polars", required=False
    )


    logger.info("starting the cleaning job")

    args = parser.parse_args()
    
    tags = [args.tag]
    if args.tag == "all":
        logger.info("Processing all tags")
        tags = ["apache-spark", "python-polars", "sql", "pyspark", "dbt", "docker", "airflow"]

    common_spark_config = {
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
    }
    if args.env == "local":
        print("This is a local execution of the capestonellm project")
        session = (
            SparkSession.builder.appName("Spark S3 Integration")
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4")
            .getOrCreate()
        )
        for tag in tags:
            clean(session, args.env, tag)
    else:
        with ClosableSparkSession("capstone_llm", spark_config=common_spark_config) as session:
            for tag in tags:
                clean(session, args.env, tag)


if __name__ == "__main__":
    
    main()
        