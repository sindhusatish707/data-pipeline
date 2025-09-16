from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import yfinance as yf
import os

default_args = {
    "owner": "you",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

def fetch_symbol(symbol="AAPL", **kwargs):
    df = yf.download(symbol, period="5d", interval="1d")
    out_dir = "/opt/airflow/data"  # create a volume or use /opt/airflow/dags/data
    os.makedirs(out_dir, exist_ok=True)
    df.to_csv(f"{out_dir}/{symbol}.csv")
    print(f"Saved {symbol}.csv")

with DAG(
    dag_id="fetch_stock_example",
    default_args=default_args,
    start_date=datetime(2024,1,1),
    schedule="@daily",
    catchup=False,
    tags=["example"],
) as dag:
    t1 = PythonOperator(
        task_id="fetch_AAPL",
        python_callable=fetch_symbol,
        op_kwargs={"symbol": "AAPL"},
    )
