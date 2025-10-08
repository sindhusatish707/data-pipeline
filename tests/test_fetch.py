import os
import pandas as pd

def test_raw_file_exists():
    raw_dir = "./data/raw"
    files = [f for f in os.listdir(raw_dir) if f.endswith(".csv")]
    assert files, "No raw CSV files found"

def test_csv_format():
    raw_dir = "./data/raw"
    files = [f for f in os.listdir(raw_dir) if f.endswith(".csv")]
    df = pd.read_csv(os.path.join(raw_dir, files[0]))
    required_cols = {"Open", "High", "Low", "Close", "Volume"}
    assert required_cols.issubset(df.columns), f"Missing columns in {files[0]}"
