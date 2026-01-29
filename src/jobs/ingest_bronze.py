import os
import sys
from datetime import datetime
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType, ArrayType

load_dotenv()

def is_already_ingested(spark, output_path, current_file_name):
    """Checks if the specific file has already been written to the Bronze layer."""
    try:
        if not os.path.exists(output_path):
            return False
            
        existing_df = spark.read.parquet(output_path)
        count = existing_df.filter(F.col("_source_file").contains(current_file_name)).count()
        return count > 0
    except Exception as e:
        print(f"Note: Bronze path not found or empty (normal for first run): {e}")
        return False

def define_bronze_schema():
    return StructType([
        StructField("id", StringType(), True),
        StructField("type", StringType(), True),
        StructField("actor", StructType([
            StructField("id", LongType(), True),
            StructField("login", StringType(), True),
            StructField("display_login", StringType(), True),
            StructField("url", StringType(), True)
        ]), True),
        StructField("repo", StructType([
            StructField("id", LongType(), True),
            StructField("name", StringType(), True),
            StructField("url", StringType(), True)
        ]), True),
        StructField("payload", StructType([
            StructField("push_id", LongType(), True),
            StructField("size", LongType(), True),
            StructField("distinct_size", LongType(), True),
            StructField("ref", StringType(), True),
            StructField("head", StringType(), True),
            StructField("before", StringType(), True),
            StructField("commits", ArrayType(StructType([
                StructField("sha", StringType(), True),
                StructField("message", StringType(), True),
                StructField("author", StructType([
                    StructField("email", StringType(), True),
                    StructField("name", StringType(), True)
                ]), True),
                StructField("url", StringType(), True)
            ])), True)
        ]), True),
        StructField("public", BooleanType(), True),
        StructField("created_at", StringType(), True)
    ])

def main(date_str, hour):
    spark = SparkSession.builder \
        .appName(f"GitHub_Bronze_Ingestion_{date_str}_{hour}") \
        .getOrCreate()

    target_file = f"{date_str}-{hour}.json.gz"
    
    # Paths from .env
    raw_dir = os.getenv("DATA_RAW_PATH", "/opt/spark/data/raw")
    input_path = os.path.join(raw_dir, target_file)
    output_path = os.getenv("DATA_BRONZE_PATH", "/opt/spark/data/bronze/events")

    # Path Verification
    if not os.path.exists(input_path):
        print(f"ERROR: Source file not found at {input_path}")
        sys.exit(1) 

    # 1. Idempotency Check
    if is_already_ingested(spark, output_path, target_file):
        print(f"SKIP: {target_file} already exists in Bronze. Exiting.")
        return

    # 2. Ingestion
    try:
        print(f"Reading raw data from: {input_path}")
        raw_df = spark.read.schema(define_bronze_schema()).json(input_path)

        # 3. Add Metadata
        bronze_df = raw_df \
            .withColumn("_ingestion_timestamp", F.current_timestamp()) \
            .withColumn("_source_file", F.lit(target_file)) 

        # 4. Write
        print(f"Writing to Bronze Layer: {output_path}")
        bronze_df.write.mode("append").parquet(output_path)
        print(f"Bronze Ingestion Complete for {target_file}!")
        
    except Exception as e:
        print(f"ERROR during Spark processing: {e}")
        sys.exit(1)

if __name__ == "__main__":
    # Hard-coded for testing
    date_input = "2024-01-01"
    hour_input = "15"

    main(date_input, hour_input)