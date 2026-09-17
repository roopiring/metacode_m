"""Q9: Download from S3, aggregate with Spark, and upload to S3."""

import logging
import os
from pathlib import Path

import pendulum
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, Param, get_current_context


BUCKET_ENV = "Q8_BUCKET"
SOURCE_KEY = "bronze/netflix_titles.csv"
LOCAL_INPUT = Path("/opt/airflow/data/q9/netflix_titles.csv")
LOCAL_OUTPUT_ROOT = Path("/opt/airflow/data/q9/silver")


def get_bucket_name() -> str:
    bucket = os.getenv(BUCKET_ENV)
    if not bucket:
        raise ValueError(f"Set the {BUCKET_ENV} environment variable")
    return bucket


def get_s3_client():
    return AwsBaseHook(
        aws_conn_id="aws_default",
        client_type="s3",
        region_name="ap-southeast-2",
    ).get_conn()


def download_from_s3() -> None:
    bucket = get_bucket_name()
    LOCAL_INPUT.parent.mkdir(parents=True, exist_ok=True)
    get_s3_client().download_file(bucket, SOURCE_KEY, str(LOCAL_INPUT))
    logging.info(
        "Downloaded s3://%s/%s -> %s (%d bytes)",
        bucket,
        SOURCE_KEY,
        LOCAL_INPUT,
        LOCAL_INPUT.stat().st_size,
    )


def upload_to_s3() -> None:
    context = get_current_context()
    logical_date = context["logical_date"].in_timezone("Asia/Seoul").to_date_string()
    local_output = LOCAL_OUTPUT_ROOT / logical_date
    parquet_files = sorted(local_output.glob("*.parquet"))
    if not parquet_files:
        raise FileNotFoundError(f"No Parquet files found in {local_output}")

    bucket = get_bucket_name()
    prefix = f"silver/{logical_date}/"
    s3_client = get_s3_client()
    for parquet_file in parquet_files:
        key = f"{prefix}{parquet_file.name}"
        s3_client.upload_file(str(parquet_file), bucket, key)
        logging.info("Uploaded %s -> s3://%s/%s", parquet_file, bucket, key)

    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    uploaded = response.get("Contents", [])
    logging.info("S3 result objects under s3://%s/%s", bucket, prefix)
    for item in uploaded:
        logging.info("key=%s size=%d bytes", item["Key"], item["Size"])
    logging.info("S3 uploaded object count: %d", len(uploaded))


with DAG(
    dag_id="weekly_pipeline_김병필",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    schedule="@weekly",
    catchup=False,
    params={
        "min_year": Param(2015, type="integer", minimum=1900, maximum=2100),
    },
    tags=["Q9", "S3", "Spark", "김병필"],
) as dag:
    download = PythonOperator(
        task_id="download_from_s3",
        python_callable=download_from_s3,
    )

    transform = BashOperator(
        task_id="transform_with_spark",
        bash_command=(
            "spark-submit /opt/airflow/dags/jobs/transform.py "
            f"--input {LOCAL_INPUT} "
            f"--output {LOCAL_OUTPUT_ROOT}/{{{{ logical_date.in_timezone('Asia/Seoul').to_date_string() }}}} "
            "--min-year {{ params.min_year }}"
        ),
    )

    upload = PythonOperator(
        task_id="upload_to_s3",
        python_callable=upload_to_s3,
    )

    download >> transform >> upload
