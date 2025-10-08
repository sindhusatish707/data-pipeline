# tests/test_transform.py
import pytest
from pyspark.sql import functions as F
from scripts.transform_spark import process_file  # refactor process_file to be importable

def test_transform_basic(tmp_path, spark):
    # create sample CSV file in tmp_path
    csv = tmp_path / "AAPL_test.csv"
    csv.write_text("Price,Close,High,Low,Open,Volume\nTicker,AAPL,AAPL,AAPL,AAPL,AAPL\nDate,,,,,\n2025-09-12,100,101,99,100,1000\n")
    outdir = tmp_path / "out"
    # call process_file(spark, str(csv), str(outdir)) - refactor script to accept outdir
    df = process_file(spark, str(csv), outdir=str(outdir))
    assert df.filter(F.col("symbol") == "AAPL").count() == 1
    assert "sma_7" in df.columns or "SMA_7" in df.columns
