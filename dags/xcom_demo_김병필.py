"""Q6: Demonstrate an automatic XCom value and a successful retry."""

import logging
from datetime import timedelta

import pendulum
from airflow.exceptions import AirflowException
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, get_current_context


def calculate_value() -> int:
    """Fail only on the first attempt, then return a calculated value."""
    context = get_current_context()
    try_number = context["ti"].try_number
    logging.info("calculate_value try_number=%s", try_number)

    if try_number == 1:
        raise AirflowException("Q6 intentional first-attempt failure")

    calculated_value = sum(range(1, 6))
    logging.info("Calculated value to return through XCom: %s", calculated_value)
    return calculated_value


def pull_and_log_value() -> None:
    """Pull the first task's return_value XCom and write it to the task log."""
    context = get_current_context()
    received_value = context["ti"].xcom_pull(
        task_ids="calculate_value",
        key="return_value",
    )

    if received_value is None:
        raise AirflowException("No XCom value was received from calculate_value")

    logging.info("xcom_pull received value: %s", received_value)


with DAG(
    dag_id="xcom_demo_김병필",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    schedule=None,
    catchup=False,
    tags=["Q6", "김병필"],
) as dag:
    calculate = PythonOperator(
        task_id="calculate_value",
        python_callable=calculate_value,
        retries=2,
        retry_delay=timedelta(seconds=15),
    )

    pull_and_log = PythonOperator(
        task_id="pull_and_log_value",
        python_callable=pull_and_log_value,
    )

    calculate >> pull_and_log
