from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from slack_utils import slack_notify
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

RAW_DIR = Variable.get("raw_data_dir", default_var="/opt/airflow/data/raw")
SCRIPT = Variable.get("transform_script", default_var="/opt/airflow/scripts/transform_spark.py")

with DAG(
    dag_id="transform_spark_dag",
    default_args=default_args,
    description="Transform stock data using PySpark",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["transform", "spark"],
) as dag:

    start = EmptyOperator(task_id="start")

    check_raw_files = BashOperator(
        task_id="check_raw_files",
        bash_command=f'if [ -z "$(ls -A {RAW_DIR}/*.csv 2>/dev/null)" ]; then echo "❌ No raw CSV files found"; exit 1; fi'
    )

    run_spark_transform = BashOperator(
        task_id="run_spark_transform",
        bash_command=f"python {SCRIPT}"
    )

    slack_task = PythonOperator(
        task_id="notify_slack",
        python_callable=lambda: slack_notify("Transformation completed ✅"),
    )

    end = EmptyOperator(task_id="end")

    start >> check_raw_files >> run_spark_transform >> slack_task >> end
