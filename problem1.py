#!/usr/bin/env python3
"""
Problem 1 — Log Level Distribution (50 pts)
Author: Siru Wu
DSAN 6000 — Fall 2025 Assignment 06 (AWS Spark Cluster Logs)

Usage on Master Node:
    uv run python problem1.py spark://<MASTER_PRIVATE_IP>:7077 --net-id <YOUR_NET_ID>

Outputs (3 files in data/output/):
     1. problem1_counts.csv
     2. problem1_sample.csv
     3. problem1_summary.txt
"""

import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    regexp_extract, col, count, rand
)

def main():
    parser = argparse.ArgumentParser(description="Problem 1: Log Level Distribution")
    parser.add_argument("master_url", help="Spark master URL, e.g. spark://10.0.0.1:7077")
    parser.add_argument("--net-id", required=True, help="Your Net ID (e.g. abc123)")
    args = parser.parse_args()

    # ------------------------------------------------------
    # 1️⃣ Initialize Spark Session
    # ------------------------------------------------------
    spark = (
        SparkSession.builder
        .appName("Problem 1 – Log Level Distribution")
        .master(args.master_url)
        # IMPORTANT: use s3a:// for Hadoop S3 access
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.InstanceProfileCredentialsProvider,com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()
    )

    net_id = args.net_id
    bucket = f"s3a://{net_id}-assignment-spark-cluster-logs"
    input_path = f"{bucket}/data/"
    output_prefix = "data/output/"

    print(f"Reading logs from: {input_path}")

    # ------------------------------------------------------
    # 2️⃣ Read all log files (recursively)
    # ------------------------------------------------------
    logs_df = spark.read.text(input_path)

    # ------------------------------------------------------
    # 3️⃣ Extract timestamp + log level + message
    # ------------------------------------------------------
    logs_parsed = logs_df.select(
        regexp_extract('value', r'^(\d{2}/\d{2}/\d{2}\s+\d{2}:\d{2}:\d{2})', 1).alias('timestamp'),
        regexp_extract('value', r'\b(INFO|WARN|ERROR|DEBUG)\b', 1).alias('log_level'),
        col('value').alias('log_entry')
    )

    # Only keep rows with identified log levels
    logs_clean = logs_parsed.filter(col('log_level') != "")

    # ------------------------------------------------------
    # 4️⃣ Compute counts per level
    # ------------------------------------------------------
    counts_df = (
        logs_clean.groupBy("log_level")
        .agg(count("*").alias("count"))
        .orderBy(col("count").desc())
    )

    # ------------------------------------------------------
    # 5️⃣ Sample 10 random entries
    # ------------------------------------------------------
    sample_df = (
        logs_clean.orderBy(rand())
        .limit(10)
        .select("log_entry", "log_level")
    )

    # ------------------------------------------------------
    # 6️⃣ Collect summary stats on driver
    # ------------------------------------------------------
    total_lines = logs_df.count()
    total_with_levels = logs_clean.count()
    unique_levels = [r[0] for r in counts_df.select("log_level").collect()]
    counts = {r["log_level"]: r["count"] for r in counts_df.collect()}

    summary_lines = []
    summary_lines.append(f"Total log lines processed: {total_lines:,}")
    summary_lines.append(f"Total lines with log levels: {total_with_levels:,}")
    summary_lines.append(f"Unique log levels found: {len(unique_levels)}")
    summary_lines.append("\nLog level distribution:")
    for lvl, cnt in counts.items():
        pct = (100.0 * cnt / total_with_levels)
        summary_lines.append(f"  {lvl:<5}: {cnt:>10,} ({pct:5.2f}%)")
    summary_text = "\n".join(summary_lines)

    # ------------------------------------------------------
    # 7️⃣ Save outputs to cluster (local path / HDFS)
    # ------------------------------------------------------
    counts_path = f"{output_prefix}/problem1_counts.csv"
    sample_path = f"{output_prefix}/problem1_sample.csv"
    summary_path = f"{output_prefix}/problem1_summary.txt"

    counts_df.coalesce(1).write.mode("overwrite").option("header", True).csv(counts_path)
    sample_df.coalesce(1).write.mode("overwrite").option("header", True).csv(sample_path)

    with open("problem1_summary.txt", "w") as f:
        f.write(summary_text)

    print("\nSaved outputs:")
    print(f"  • {counts_path}")
    print(f"  • {sample_path}")
    print(f"  • {summary_path}")

    spark.stop()


if __name__ == "__main__":
    main()
