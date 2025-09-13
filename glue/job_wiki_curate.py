# glue/job_wiki_curate.py
"""
Curate Wikimedia RecentChanges:
- Read raw NDJSON (one JSON per line) from S3
- Normalize a proper timestamp
- Add date partitions (yyyy/mm/dd)
- Write Parquet to curated/ partitioned by yyyy/mm/dd

Notes:
- repartition(1) is for demo-size outputs; remove or tune for real workloads.
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, to_timestamp, from_unixtime, year, month, dayofmonth

# Required args are provided by the Glue Job / Airflow operator
args = getResolvedOptions(sys.argv, ["JOB_NAME", "RAW_PATH", "CURATED_PATH"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Read raw wiki events from S3 (e.g., s3://.../raw/ds=YYYY-MM-DD/)
raw_df = spark.read.json(args["RAW_PATH"])

# Derive a proper timestamp and calendar partitions
df = (
    raw_df
    .withColumn("ts", to_timestamp(from_unixtime(col("timestamp"))))  # epoch->timestamp
    .withColumn("yyyy", year(col("ts")))
    .withColumn("mm", month(col("ts")))
    .withColumn("dd", dayofmonth(col("ts")))
)

# Write curated parquet, partitioned by yyyy/mm/dd
(
    df
    .repartition(1)  # demo-friendly small files
    .write
    .mode("append")
    .partitionBy("yyyy", "mm", "dd")
    .parquet(args["CURATED_PATH"])  # e.g., s3://.../curated/
)

job.commit()
