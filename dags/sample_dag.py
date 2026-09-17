"""Q3 sample DAG: start, two Python tasks, and end in sequence."""

import pendulum

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG


def first_message() -> str:
    """Return a value so it is recorded in the task log and XCom."""
    return "김병필: 첫 번째 Python 작업 완료"


def second_message() -> str:
    """Return a second value so it is recorded in the task log and XCom."""
    return "김병필: 두 번째 Python 작업 완료"


with DAG(
    dag_id="sample_dag",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["Q3", "김병필"],
) as dag:
    start = EmptyOperator(task_id="start")

    python_task_1 = PythonOperator(
        task_id="python_task_1",
        python_callable=first_message,
    )

    python_task_2 = PythonOperator(
        task_id="python_task_2",
        python_callable=second_message,
    )

    end = EmptyOperator(task_id="end")

    start >> python_task_1 >> python_task_2 >> end
