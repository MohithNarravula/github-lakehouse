import os
import sys
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

load_dotenv()

def main(date_str):
    spark = SparkSession.builder \
        .appName(f"GitHub_Silver_Transformation_{date_str}") \
        .getOrCreate()

    # 1. Read from Bronze
    bronze_path = os.getenv("DATA_BRONZE_PATH", "/opt/spark/data/bronze/events")
    
    # Path Verification
    if not os.path.exists(bronze_path):
        print(f"ERROR: Bronze path {bronze_path} not found. Run ingestion first.")
        sys.exit(1)

    print(f"Reading from Bronze Layer for date: {date_str}")
    bronze_df = spark.read.parquet(bronze_path)

    # 2. Filter for PushEvents and specific date
    # Casting created_at to date for accurate filtering
    clean_bronze_df = bronze_df.filter(
        (F.col("type") == "PushEvent") & 
        (F.to_date(F.col("created_at")) == F.lit(date_str))
    ).dropDuplicates(["id"])

    # 3. Flattening Nested Commits
    # Explode turns the list of commits into individual rows
    exploded_df = clean_bronze_df.withColumn("commit", F.explode(F.col("payload.commits")))

    silver_df = exploded_df.select(
        F.col("id").alias("event_id"),
        F.to_timestamp(F.col("created_at")).alias("event_timestamp"),
        F.col("actor.login").alias("user_name"),
        F.col("repo.name").alias("repo_name"),
        F.col("commit.sha").alias("commit_sha"),
        F.col("commit.message").alias("commit_message"),
        F.col("commit.author.email").alias("author_email"),
        F.to_date(F.col("created_at")).alias("event_date")
    )

    # 4. Write to Silver
    output_path = os.getenv("DATA_SILVER_PATH", "/opt/spark/data/silver/commits")
    
    # Set dynamic partition overwrite mode
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    
    print(f"Writing to Silver Layer: {output_path} (Partition: {date_str})")
    silver_df.write.mode("overwrite") \
             .partitionBy("event_date") \
             .parquet(output_path)

    print(f"Silver Transformation Complete for {date_str}!")

if __name__ == "__main__":
    # Hard-coded for testing
    date_input = "2024-01-01"
    main(date_input)