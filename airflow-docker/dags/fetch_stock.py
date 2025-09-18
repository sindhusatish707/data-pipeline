from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import yfinance as yf
import os

# List of tickers to fetch
TICKERS = ["AAPL", "MSFT", "TSLA"]

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

def fetch_symbols(tickers, **kwargs):
    today = datetime.today().strftime("%Y-%m-%d")
    out_dir = "/opt/airflow/data/raw"
    os.makedirs(out_dir, exist_ok=True)
    for symbol in tickers:
        df = yf.download(symbol, period="5d", interval="1d")
        path = f"{out_dir}/{symbol}_{today}.csv"
        df.to_csv(path)
        print(f"✅ Saved {symbol}.csv to {path}")

with DAG(
    dag_id="fetch_multiple_stocks",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["data-extraction"],
) as dag:
    PythonOperator(
        task_id="fetch_stock_data",
        python_callable=fetch_symbols,
        op_kwargs={"tickers": TICKERS},
    )
