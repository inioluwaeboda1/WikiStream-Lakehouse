# wiki_pipeline_dag.py
# Wikipedia edits → S3 (raw) → Glue (curated) → Athena (CTAS) → RDS.
# Env-driven, idempotent per Airflow `ds`. Works with providers-amazon 8.25.x.

from datetime import datetime, timedelta
import os, subprocess
from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.providers.amazon.aws.hooks.athena import AthenaHook
from airflow.operators.python import PythonOperator
from airflow.models.baseoperator import chain

AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
GLUE_DB = "wiki_db"
GLUE_CRAWLER = "wiki_raw_crawler"
GLUE_JOB = "wiki_curate_job"

S3_BUCKET = os.getenv("S3_BUCKET", "wiki-pipline-bucket")
RAW_PATH = f"s3://{S3_BUCKET}/raw/"
CURATED_PATH = f"s3://{S3_BUCKET}/curated/"
ATHENA_WG = "primary"
ATHENA_OUTPUT = f"s3://{S3_BUCKET}/athena-results/"
GLUE_SCRIPT = f"s3://{S3_BUCKET}/scripts/job_wiki_curate.py"

def make_daily_ctas(**context):
    ds = context["ds"]
    q = f"""
    CREATE TABLE IF NOT EXISTS wiki_db.daily_top_titles_{ds.replace('-','_')} AS
    SELECT
      date(from_unixtime(timestamp)) AS ds,
      title,
      "user" AS user,
      count(*) AS edits
    FROM curated
    WHERE date(from_unixtime(timestamp)) = date('{ds}')
    GROUP BY 1,2,3
    """
    AthenaHook(region_name=AWS_REGION).run_query(
        query=q,
        query_context={"Database": GLUE_DB},
        result_configuration={"OutputLocation": ATHENA_OUTPUT},
    )

def load_rds_callable(**context):
    env = os.environ.copy()
    env.setdefault("AWS_REGION", AWS_REGION)
    env.setdefault("ATHENA_DB", GLUE_DB)
    env.setdefault("ATHENA_OUT", ATHENA_OUTPUT)
    env.setdefault("TARGET_DATE", context["ds"])
    subprocess.run(
        ["python", "/opt/airflow/rds/upsert_athena_aggregates.py"],
        check=True,
        env=env,
    )

default_args = {"owner": "data-eng", "retries": 1, "retry_delay": timedelta(minutes=5)}

with DAG(
    dag_id="wiki_pipeline",
    start_date=datetime(2025, 9, 1),
    schedule="0 1 * * *",  # 01:00 UTC daily
    catchup=False,
    default_args=default_args,
    tags=["kafka", "glue", "athena", "rds"],
) as dag:

    crawl_raw = GlueCrawlerOperator(
        task_id="crawl_raw",
        config={"Name": GLUE_CRAWLER},
        wait_for_completion=True,
    )

    curate_job = GlueJobOperator(
        task_id="curate_job",
        job_name=GLUE_JOB,
        script_location=GLUE_SCRIPT,  # used on first bootstrap
        script_args={"--RAW_PATH": RAW_PATH, "--CURATED_PATH": CURATED_PATH},
        wait_for_completion=True,
    )

    athena_partition_repair = AthenaOperator(
        task_id="athena_msck_repair",
        query="MSCK REPAIR TABLE curated",
        database=GLUE_DB,
        output_location=ATHENA_OUTPUT,
        workgroup=ATHENA_WG,
    )

    daily_ctas = PythonOperator(task_id="daily_ctas", python_callable=make_daily_ctas)
    load_rds = PythonOperator(task_id="load_rds", python_callable=load_rds_callable)

    chain(crawl_raw, curate_job, athena_partition_repair, daily_ctas, load_rds)
