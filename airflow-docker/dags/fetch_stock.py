from airflow import DAG
from airflow.operators.python import PythonOperator, get_current_context
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
from datetime import datetime, timedelta
from slack_utils import slack_notify
import yfinance as yf
import os

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

def fetch_symbols_task(**kwargs):
    ctx = get_current_context()
    dag_run = ctx.get("dag_run")
    conf = dag_run.conf if dag_run else {}

    # symbols: dag_run.conf > Airflow Variable > fallback
    symbols_conf = conf.get("symbols")
    symbols_str = symbols_conf if symbols_conf else Variable.get("symbols", default_var="AAPL")
    symbols = [s.strip() for s in symbols_str.split(",") if s.strip()]

    # start/end dates
    start = conf.get("start_date") or Variable.get("fetch_start_date", default_var=None)
    end = conf.get("end_date") or Variable.get("fetch_end_date", default_var=None)

    def parse_date(x):
        return datetime.fromisoformat(x).date() if x else None
    start_date = parse_date(start) if start else None
    end_date = parse_date(end) if end else None

    out_dir = "/opt/airflow/data/raw"
    os.makedirs(out_dir, exist_ok=True)

    for symbol in symbols:
        df = yf.download(symbol, start=start_date, end=end_date, period=None if start else "5d", interval="1d")
        today = datetime.today().strftime("%Y-%m-%d")
        path = f"{out_dir}/{symbol}_{today}.csv"
        df.to_csv(path)
        print(f"✅ Saved {symbol}.csv to {path}")

with DAG(
    dag_id="fetch_stock_dag",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["extract", "stocks"],
) as dag:
    
    start = EmptyOperator(task_id="start")

    fetch_task = PythonOperator(
        task_id="fetch_stock_data",
        python_callable=fetch_symbols_task,
    )

    slack_task = PythonOperator(
        task_id="notify_slack",
        python_callable=lambda: slack_notify("Extraction completed ✅"),
    )

    end = EmptyOperator(task_id="end")

    start >> fetch_task >> slack_task >> end
