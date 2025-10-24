#!/usr/bin/env python3
"""
Problem 2 — Cluster Usage Analysis (Spark Version)
Author: Siru Wu
DSAN 6000 — Fall 2025 Assignment 06 (AWS Spark Cluster Logs)

Usage:
    uv run python problem2.py spark://<MASTER_PRIVATE_IP>:7077 --net-id <YOUR_NET_ID>
    uv run python problem2.py --skip-spark      # 快速模式：直接用已有输出重画图
"""

import os, glob, argparse, re
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import regexp_extract, col, min as spark_min, max as spark_max, count

# ------------------------------------------------------------
# Helper to locate .csv from Spark output
# ------------------------------------------------------------
def find_csv_in_dir(base_dir, keyword=None):
    """
    更智能：根据 keyword 和文件大小判断哪个 CSV 属于哪个输出
    """
    pattern = os.path.join(base_dir, "**", "part-*.csv")
    candidates = glob.glob(pattern, recursive=True)
    if not candidates:
        raise FileNotFoundError(f"No CSV part file found under {base_dir}")

    if keyword == "cluster_summary":
        # 通常 cluster_summary 文件较小（<1KB）
        filtered = [f for f in candidates if os.path.getsize(f) < 2000]
    elif keyword == "timeline":
        # timeline 文件通常较大
        filtered = [f for f in candidates if os.path.getsize(f) >= 2000]
    else:
        filtered = [f for f in candidates if keyword in f] if keyword else candidates

    if filtered:
        print(f"✅ Using {keyword}: {filtered[0]}")
        return filtered[0]

    print(f"⚠️ No match for keyword={keyword}, fallback to first file.")
    return candidates[0]


# ------------------------------------------------------------
# Plotting helper
# ------------------------------------------------------------
def create_plots(cluster_summary_csv, timeline_csv, output_dir):
    sns.set(style="whitegrid", font_scale=1.1)
    os.makedirs(output_dir, exist_ok=True)

    # 自动找到真正的 CSV 文件
    cluster_summary_csv = find_csv_in_dir("data/output", "cluster_summary")
    timeline_csv = find_csv_in_dir("data/output", "timeline")

    print("✅ Using cluster summary:", cluster_summary_csv)
    print("✅ Using timeline:", timeline_csv)

    cs = pd.read_csv(cluster_summary_csv)
    tl = pd.read_csv(timeline_csv)

    # 尝试解析时间列
    for c in ["start_time", "end_time"]:
        if c in tl.columns:
            tl[c] = pd.to_datetime(tl[c], errors="coerce")

    # 计算持续时间
    if "start_time" in tl.columns and "end_time" in tl.columns:
        tl["duration_sec"] = (tl["end_time"] - tl["start_time"]).dt.total_seconds()
    else:
        print("⚠️ No start_time/end_time columns found; skipping duration plot.")

    # 1️⃣ 柱状图
    plt.figure(figsize=(8,5))
    sns.barplot(data=cs, x="cluster_id", y="num_applications", palette="Blues_d")
    plt.title("Number of Applications per Cluster")
    plt.xlabel("Cluster ID")
    plt.ylabel("Applications")
    plt.xticks(rotation=45)
    for i, v in enumerate(cs["num_applications"]):
        plt.text(i, v + 0.3, str(v), ha='center', va='bottom')
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "problem2_bar_chart.png"))
    plt.close()

    # 2️⃣ 持续时间分布图
    if "duration_sec" in tl.columns:
        largest = cs.sort_values("num_applications", ascending=False).iloc[0]["cluster_id"]
        subset = tl[tl["cluster_id"] == largest]
        plt.figure(figsize=(8,5))
        sns.histplot(subset["duration_sec"], kde=True, bins=30)
        plt.xscale("log")
        plt.title(f"Job Duration Distribution (Cluster {largest}, n={len(subset)})")
        plt.xlabel("Duration (seconds, log scale)")
        plt.ylabel("Frequency")
        plt.tight_layout()
        plt.savefig(os.path.join(output_dir, "problem2_density_plot.png"))
        plt.close()

    print("✅ Saved: problem2_bar_chart.png, problem2_density_plot.png")


# ------------------------------------------------------------
# Main
# ------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Problem 2: Cluster Usage Analysis (Spark)")
    parser.add_argument("master_url", nargs="?", default=None)
    parser.add_argument("--net-id", required=False, default="sw1430")
    parser.add_argument("--skip-spark", action="store_true")
    args = parser.parse_args()

    output_dir = "data/output"
    os.makedirs(output_dir, exist_ok=True)

    # ✅ Quick mode
    if args.skip_spark:
        print("Skipping Spark processing, rebuilding visualizations...")
        create_plots(
            f"{output_dir}/problem2_cluster_summary.csv",
            f"{output_dir}/problem2_timeline.csv",
            output_dir
        )
        return

    # --------------------------------------------------------
    # 1️⃣ Spark session
    # --------------------------------------------------------
    spark = (
        SparkSession.builder
        .appName("Problem 2 – Cluster Usage Analysis")
        .master(args.master_url)
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.InstanceProfileCredentialsProvider,"
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
        .getOrCreate()
    )

    net_id = args.net_id
    bucket = f"s3a://{net_id}-assignment-spark-cluster-logs"
    input_path = f"{bucket}/data/"
    print(f"📦 Reading logs from {input_path}")

    # --------------------------------------------------------
    # 2️⃣ Read + extract patterns
    # --------------------------------------------------------
    df = spark.read.text(input_path).withColumnRenamed("value", "line")

    df = (
        df.withColumn("cluster_id", regexp_extract("line", r"application_(\d+)_\d+", 1))
            .withColumn("application_id", regexp_extract("line", r"(application_\d+_\d+)", 1))
            .withColumn("start_time_raw", regexp_extract("line", r"(\d{2}/\d{2}/\d{2}\s+\d{2}:\d{2}:\d{2})", 1))
            .withColumn("end_time_raw", regexp_extract("line", r"(\d{2}/\d{2}/\d{2}\s+\d{2}:\d{2}:\d{2})", 1))
            .withColumn("start_time", F.try_to_timestamp("start_time_raw", F.lit("yy/MM/dd HH:mm:ss")))
            .withColumn("end_time", F.try_to_timestamp("end_time_raw", F.lit("yy/MM/dd HH:mm:ss")))
            .filter(col("application_id") != "")
            .filter(col("start_time").isNotNull() & col("end_time").isNotNull())
            .drop("start_time_raw", "end_time_raw")
    )

    # --------------------------------------------------------
    # 3️⃣ Aggregation
    # --------------------------------------------------------
    timeline_df = (
        df.groupBy("cluster_id", "application_id")
          .agg(spark_min("start_time").alias("start_time"),
               spark_max("end_time").alias("end_time"))
          .orderBy("cluster_id", "start_time")
    )
    cluster_summary = (
        timeline_df.groupBy("cluster_id")
          .agg(count("*").alias("num_applications"),
               spark_min("start_time").alias("cluster_first_app"),
               spark_max("end_time").alias("cluster_last_app"))
          .orderBy(col("num_applications").desc())
    )

    timeline_path = f"{bucket}/output/problem2_timeline.csv"
    cluster_summary_path = f"{bucket}/output/problem2_cluster_summary.csv"

    timeline_df.coalesce(1).write.mode("overwrite").option("header", True).csv(timeline_path)
    cluster_summary.coalesce(1).write.mode("overwrite").option("header", True).csv(cluster_summary_path)

    # --------------------------------------------------------
    # 4️⃣ Statistics
    # --------------------------------------------------------
    total_clusters = cluster_summary.count()
    total_apps = timeline_df.count()
    avg_apps_per_cluster = total_apps / total_clusters if total_clusters else 0
    summary_text = (
        f"Total unique clusters: {total_clusters}\n"
        f"Total applications: {total_apps}\n"
        f"Average applications per cluster: {avg_apps_per_cluster:.2f}\n"
    )

    summary_path = os.path.join(output_dir, "problem2_stats.txt")
    with open(summary_path, "w") as f:
        f.write(summary_text)

    print(summary_text)
    print("Saved outputs:")
    print(f"  {timeline_path}\n  {cluster_summary_path}\n  {summary_path}")

    # --------------------------------------------------------
    # 5️⃣ Download from S3 to local for plotting
    # --------------------------------------------------------
    print("📦 Downloading Spark outputs from S3 to local data/output/...")
    os.system(f"aws s3 cp {timeline_path.replace('s3a://', 's3://')} {output_dir}/ --recursive --exclude '*' --include 'part-*.csv'")
    os.system(f"aws s3 cp {cluster_summary_path.replace('s3a://', 's3://')} {output_dir}/ --recursive --exclude '*' --include 'part-*.csv'")
    print("✅ Successfully downloaded and renamed part files from S3.")

    # --------------------------------------------------------
    # 6️⃣ Plot
    # --------------------------------------------------------
    create_plots(
        timeline_csv="data/output",
        cluster_summary_csv="data/output",
        output_dir="data/output"
    )

    spark.stop()

if __name__ == "__main__":
    main()
