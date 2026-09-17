"""Q7: Fill seven past daily intervals and write one file per logical date."""

import logging
from pathlib import Path

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG


OUTPUT_ROOT = Path("/opt/airflow/data/backfill")


def write_logical_date_file(logical_date_ds: str) -> str:
    """Write the result under a directory named for this run's logical date."""
    dated_directory = OUTPUT_ROOT / logical_date_ds
    dated_directory.mkdir(parents=True, exist_ok=True)
    result_file = dated_directory / "result.txt"
    result_file.write_text(
        f"dag_id=backfill_demo_김병필\nlogical_date={logical_date_ds}\n",
        encoding="utf-8",
    )
    logging.info("Wrote logical-date output: %s", result_file)
    return str(result_file)


with DAG(
    dag_id="backfill_demo_김병필",
    start_date=pendulum.datetime(2026, 9, 10, tz="Asia/Seoul"),
    schedule="@daily",
    catchup=True,
    max_active_runs=2,
    tags=["Q7", "김병필", "backfill"],
) as dag:
    write_result = PythonOperator(
        task_id="write_logical_date_file",
        python_callable=write_logical_date_file,
        op_kwargs={"logical_date_ds": "{{ ds }}"},
    )
