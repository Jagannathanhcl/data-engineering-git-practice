from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import yaml

def main():
    spark = SparkSession.builder \
        .appName("ConfigDrivenSparkJob") \
        .getOrCreate()

    input_path = "gs://jagan_git/customer_table.csv"
    output_path = "gs://jagan_git/output/processed_data"

    df = spark.read.csv(input_path, header=True, inferSchema=True)

    total_count = df.count()
    bad_count = df.filter(df["age"].isNull()).count()

    if bad_count > 0:
        print("⚠️ DATA QUALITY ALERT")
        print(f"Found {bad_count} bad records")

    df = df.dropna(subset=["age"])

    print(f"✅ Cleaned data. Before: {total_count}, After: {df.count()}")

    df.write.mode("overwrite").csv(output_path, header=True)

    spark.stop()
