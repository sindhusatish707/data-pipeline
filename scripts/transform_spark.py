from pyspark.sql import SparkSession, functions as F, Window
from pyspark.sql.types import DoubleType, LongType
import os, glob, shutil

RAW_GLOB = "/opt/airflow/data/raw/*.csv"
OUT_DIR = "/opt/airflow/data/processed"

def process_file(spark, file_path):
    print(f"📄 Processing file: {file_path}")

    raw_df = spark.read.csv(file_path, header=False)

    # Extract ticker from row 2, col 1
    ticker_row = raw_df.filter(F.col("_c0") == "Ticker").limit(1)
    ticker = ticker_row.collect()[0]["_c1"] if ticker_row.count() > 0 else "UNKNOWN"

    # Keep only rows where _c0 is a date (not "Price", "Ticker", "Date")
    cleaned_df = raw_df.filter(
        (F.col("_c0") != "Price") &
        (F.col("_c0") != "Ticker") &
        (F.col("_c0") != "Date")
    )

    # Rename columns based on your file layout
    df = cleaned_df.select(
        F.col("_c0").alias("Date"),
        F.col("_c1").alias("Close"),
        F.col("_c2").alias("High"),
        F.col("_c3").alias("Low"),
        F.col("_c4").alias("Open"),
        F.col("_c5").alias("Volume")
    )

    df = df.withColumn("Symbol", F.lit(ticker))

    # Convert data types
    df = (
        df.withColumn("Date", F.to_date("Date", "yyyy-MM-dd"))
          .withColumn("Open", F.col("Open").cast(DoubleType()))
          .withColumn("High", F.col("High").cast(DoubleType()))
          .withColumn("Low", F.col("Low").cast(DoubleType()))
          .withColumn("Close", F.col("Close").cast(DoubleType()))
          .withColumn("Volume", F.col("Volume").cast(LongType()))
    )

    # Deduplicate and compute metrics
    w_dup = Window.partitionBy("Symbol", "Date").orderBy(F.col("Volume").desc_nulls_last())
    df = df.withColumn("_row_rank", F.row_number().over(w_dup)).filter(F.col("_row_rank") == 1).drop("_row_rank")

    w = Window.partitionBy("Symbol").orderBy("Date")
    df = df.withColumn("Prev_Close", F.lag("Close").over(w))
    df = df.withColumn("Daily_Return",
                       F.when(F.col("Prev_Close").isNotNull(),
                              (F.col("Close") - F.col("Prev_Close")) / F.col("Prev_Close")))

    w7 = Window.partitionBy("Symbol").orderBy("Date").rowsBetween(-6, 0)
    w14 = Window.partitionBy("Symbol").orderBy("Date").rowsBetween(-13, 0)
    df = df.withColumn("SMA_7", F.avg("Close").over(w7))
    df = df.withColumn("SMA_14", F.avg("Close").over(w14))
    df = df.withColumn("Volatility_7", F.stddev("Close").over(w7))

    df = df.withColumn("Year", F.year("Date")).withColumn("Month", F.month("Date"))

    final_cols = ["Symbol", "Date", "Year", "Month", "Open", "High", "Low", "Close", "Volume",
                  "Prev_Close", "Daily_Return", "SMA_7", "SMA_14", "Volatility_7"]
    df = df.select(*final_cols)

    df.write.mode("append").partitionBy("Symbol", "Year", "Month").parquet(OUT_DIR)

    print(f"✅ Processed {ticker} with {df.count()} rows")
    df.show(5, truncate=False)

def main():
    spark = (
        SparkSession.builder
        .appName("stock_transform")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    if os.path.exists(OUT_DIR):
        print(f"🧹 Clearing old processed data in {OUT_DIR}")
        shutil.rmtree(OUT_DIR)

    csv_files = glob.glob(RAW_GLOB)
    if not csv_files:
        print("❌ No CSV files found!")
        return

    for file in csv_files:
        process_file(spark, file)

    print(f"🎉 All files processed. Output saved to {OUT_DIR}")
    spark.stop()

if __name__ == "__main__":
    main()
