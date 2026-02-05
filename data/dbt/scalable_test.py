import argparse
import logging
import sys
import os
import json
import uuid
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode

# Configure logging
logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger(__name__)

# --- 1. Define the worker function ---
# This function runs on the worker nodes (or threads in local mode).
# It processes a "chunk" (partition) of rows at a time.
def write_partition_files(partition_iterator):
    # Imports must be inside the function for distributed workers to find them
    import os
    import json
    import uuid
    
    # Define output directory (hardcoded for simplicity here, 
    # but normally passed via closure or config)
    output_dir = "output/dbt_dataset"
    
    # Safety check: Ensure directory exists. 
    # (Note: In a real cluster, workers might not share the same local disk, 
    # so you'd normally write to S3/HDFS here.)
    os.makedirs(output_dir, exist_ok=True)
    
    # Loop through the chunk of data assigned to this worker
    for row in partition_iterator:
        record = row.asDict()
        
        # Create a unique filename using UUID to avoid collisions
        # (Since multiple workers run at the same time, simple counting doesn't work well)
        file_name = f"pair_{uuid.uuid4()}.json"
        full_path = os.path.join(output_dir, file_name)
        
        with open(full_path, "w", encoding='utf-8') as f:
            json.dump(record, f, indent=4)

def clean(spark: SparkSession, env: str, tag: str):
    logger.info(f"Processing tag: {tag} in environment: {env}")
    
    questions_path = "questions.json"
    answers_path = "answers.json"

    try:
        df_q_raw = spark.read.option("multiLine", True).json(questions_path)
        df_a_raw = spark.read.option("multiLine", True).json(answers_path)

        # Flatten Questions
        df_q = df_q_raw.select(explode(col("items")).alias("item")).select(
            col("item.question_id").alias("question_id"),
            col("item.title").alias("title"),
            col("item.body").alias("question_body")
        )

        # Flatten Answers
        df_a = df_a_raw.select(explode(col("items")).alias("item")).select(
            col("item.question_id").alias("question_id"),
            col("item.body").alias("answer_body")
        )

        # Join
        df_joined = df_q.join(df_a, on="question_id", how="inner")
        final_df = df_joined.select("title", "question_body", "answer_body")

        logger.info("Starting distributed write (foreachPartition)...")

        # --- 2. Execute Scalable Write ---
        # Instead of collecting data to the driver, we send the 
        # 'write_partition_files' function to the data.
        final_df.rdd.foreachPartition(write_partition_files)
        
        logger.info("Distributed write completed.")

    except Exception as e:
        logger.error(f"Error processing data: {e}")
        raise e

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-e", "--env", default="local")
    parser.add_argument("-t", "--tag", default="dbt")
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("Capstone Local ETL")
        .master("local[*]") 
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )

    clean(spark, args.env, args.tag)
    spark.stop()

if __name__ == "__main__":
    main()