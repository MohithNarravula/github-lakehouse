import os
import sys
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

load_dotenv()

def main(date_str):
    spark = SparkSession.builder \
        .appName(f"GitHub_Gold_Analytics_{date_str}") \
        .getOrCreate()

    # 1. Read from Silver
    silver_path = os.getenv("DATA_SILVER_PATH", "/opt/spark/data/silver/commits")
    
    if not os.path.exists(silver_path):
        print(f"ERROR: Silver path {silver_path} not found. Run transformation first.")
        sys.exit(1)

    print(f"Reading from Silver Layer for date: {date_str}")
    silver_df = spark.read.parquet(silver_path).filter(F.col("event_date") == F.lit(date_str))

    # 2. Aggregate: Daily Repository Leaderboard
    gold_df = silver_df.groupBy("repo_name", "event_date") \
        .agg(
            F.count("commit_sha").alias("total_commits"),
            F.countDistinct("user_name").alias("unique_contributors")
        ) \
        .orderBy(F.desc("total_commits"))

    # 3. Write to Gold
    output_path = os.getenv("DATA_GOLD_PATH", "/opt/spark/data/gold/daily_repo_metrics")
    
    # We use 'overwrite' for the specific date partition
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    
    print(f"Writing to Gold Layer: {output_path} (Partition: {date_str})")
    gold_df.write.mode("overwrite").partitionBy("event_date").parquet(output_path)

    print(f"Gold Analytics for {date_str}:")
    gold_df.show(5, False)

if __name__ == "__main__":
    # Hard-coded for testing
    date_input = "2024-01-01"
    main(date_input)