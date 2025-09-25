import pandas as pd
from sqlalchemy import create_engine, text
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import os, glob

PROCESSED_DIR = "/opt/airflow/data/processed"
CONN_STR = "postgresql+psycopg2://airflow:airflow@postgres/airflow"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

def load_to_postgres():
    parquet_files = glob.glob(f"{PROCESSED_DIR}/**/*.parquet", recursive=True)
    if not parquet_files:
        raise FileNotFoundError("No processed parquet files found.")

    engine = create_engine(CONN_STR)

    create_table_sql = """
    CREATE TABLE IF NOT EXISTS trades (
        symbol TEXT,
        date DATE,
        year INT,
        month INT,
        open DOUBLE PRECISION,
        high DOUBLE PRECISION,
        low DOUBLE PRECISION,
        close DOUBLE PRECISION,
        volume BIGINT,
        prev_close DOUBLE PRECISION,
        daily_return DOUBLE PRECISION,
        sma_7 DOUBLE PRECISION,
        sma_14 DOUBLE PRECISION,
        volatility_7 DOUBLE PRECISION
    );
    """
    with engine.begin() as conn:
        conn.execute(text(create_table_sql))

    df = pd.concat([pd.read_parquet(f) for f in parquet_files], ignore_index=True)

    # Normalize column names to lowercase
    df.columns = [c.lower() for c in df.columns]

    df.to_sql("trades", engine, if_exists="append", index=False)
    print(f"✅ Loaded {len(df)} rows into Postgres trades table")

with DAG(
    dag_id="load_stock_dag",
    default_args=default_args,
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["load", "postgres"],
) as dag:

    start = EmptyOperator(task_id="start")
    load_task = PythonOperator(
        task_id="load_to_postgres",
        python_callable=load_to_postgres
    )
    end = EmptyOperator(task_id="end")

    start >> load_task >> end
