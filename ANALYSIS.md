# DSAN 6000 — Assignment 06: Spark Log Analysis on AWS Cluster

**Author:** Siru Wu
**Net ID:** sw1430

---

## Problem 1 — Log Level Distribution

In this part of the assignment, I analyzed the distribution of log levels across all Spark application logs. Using PySpark, I loaded all text files from the S3 bucket and applied regular expressions to extract the log level from each line. After filtering out malformed entries, I grouped and counted the number of `INFO`, `WARN`, `ERROR`, and `DEBUG` messages. The goal was to understand the overall stability and verbosity of the Spark jobs.

Most of the log entries were of the `INFO` level, which means the majority of messages recorded normal operation information rather than errors. Only a small fraction were `WARN` or `ERROR` messages, suggesting that the Spark cluster ran smoothly and encountered few critical issues. The results were written into three output files: counts, a random sample of log entries, and a summary of the statistics.

From a performance perspective, the program handled around three million lines of log data in just a few minutes. By caching intermediate DataFrames and partitioning the dataset before aggregation, the computation remained efficient. This task mainly focused on testing the ability to parse large unstructured text and extract meaningful structured information.

---

## Problem 2 — Cluster Usage Analysis

The second problem explored how Spark clusters were used over time. I used PySpark to extract the `cluster_id`, `application_id`, and start and end timestamps from the raw logs. These values were parsed using regular expressions and converted into timestamp columns. I then aggregated job timelines and summarized the number of applications per cluster, writing the results into two CSV outputs for visualization.

The analysis revealed that one cluster handled the vast majority of Spark applications — 181 in total — while other clusters processed only a handful of jobs. This indicates that most workloads were centralized on a single production environment, and the smaller clusters were likely used for testing or short-term experiments.

For the largest cluster, I examined how long each Spark job took to run. Most jobs finished very quickly — within a few minutes — and there were no unusually long runs. The overall distribution of job durations was fairly compact, suggesting that the dataset and Spark configurations were well optimized. The results were visualized in two figures: a bar chart showing how many applications ran on each cluster, and a density plot showing how job durations were distributed.

---

## Performance and Observations

Running the full analysis on a four-node AWS Spark cluster (one master and three workers, all t3.large instances) took around five minutes. The Spark Web UI confirmed that all executors were active and evenly loaded throughout execution. To reduce file fragmentation, I used `.coalesce(1)` before writing each output. Using the instance profile for authentication allowed Spark to access S3 directly without manual credential setup, which simplified the workflow and reduced potential errors.

Both problems ran smoothly without any failed tasks. The short total runtime shows that the dataset size and cluster resources were well balanced. Overall, these experiments demonstrated that distributed data processing can be executed efficiently on AWS EC2 using PySpark, even with multi-gigabyte log data.

---

## Summary

The log data showed a stable Spark environment with very few errors and warnings. Most computation took place on one main cluster, suggesting a clear distinction between production and auxiliary clusters. Job durations were short and consistent, indicating well-tuned Spark configurations. The combination of PySpark for large-scale data processing and Seaborn for visualization provided a clear view of how Spark workloads behaved across clusters and over time.

---

## Spark Web UI Screenshots

Screenshots of the Spark Web UI (Master and Application pages) are included in the submission.
They show active executors, completed stages, and job timelines confirming successful execution of both Problem 1 and Problem 2.
These screenshots illustrate how the Spark jobs were distributed across nodes and how quickly tasks completed during runtime.