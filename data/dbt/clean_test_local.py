import argparse
import logging
import sys
import os
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode

# Configure logging
logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger(__name__)

def clean(spark: SparkSession, env: str, tag: str):
    logger.info(f"Processing tag: {tag} in environment: {env}")

    # 1. Define paths 
    questions_path = "questions.json"
    answers_path = "answers.json"
    # This will now be a directory where we dump many individual files
    output_dir = f"output/{tag}_dataset"

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

        # Join to create Q&A Pairs
        df_joined = df_q.join(df_a, on="question_id", how="inner")

        # Select Exact Fields
        final_df = df_joined.select("title", "question_body", "answer_body")

        # --- CHANGED SECTION START ---
        
        logger.info("Collecting data to local driver...")
        # 1. Collect the distributed data into a Python list
        # (Only do this for local/small datasets, not massive production data)
        results = final_df.collect()
        
        logger.info(f"Generated {len(results)} Q&A pairs. Writing individual files...")

        # 2. Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)

        # 3. Loop through and write 1 file per row
        for index, row in enumerate(results):
            # Convert Spark Row to Python Dictionary
            data_dict = row.asDict()
            
            # Create a unique filename (e.g., pair_0.json, pair_1.json)
            file_name = f"pair_{index}.json"
            full_path = os.path.join(output_dir, file_name)
            
            with open(full_path, "w", encoding='utf-8') as f:
                # indent=4 makes it pretty-printed (easier to read)
                json.dump(data_dict, f, indent=4)
                
        logger.info(f"Successfully wrote {len(results)} individual JSON files to {output_dir}")

        # --- CHANGED SECTION END ---

    except Exception as e:
        logger.error(f"Error processing data: {e}")
        raise e

def main():
    parser = argparse.ArgumentParser(description="capstone_llm_local")
    parser.add_argument("-e", "--env", dest="env", default="local", required=False)
    parser.add_argument("-t", "--tag", dest="tag", default="dbt", required=False)
    
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