from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import yaml

def main():
    spark = SparkSession.builder \
        .appName("ConfigDrivenSparkJob") \
        .getOrCreate()

    # Read config file
    with open("config/config.yaml", "r") as file:
        config = yaml.safe_load(file)

       input_path = "gs://jagan_git-data-bucket/customer_table.csv"
        output_path = "gs://jagan_git-data-bucket/output/processed_data"

    min_age = config["min_age"]

    # Read data
    df = spark.read.csv(input_path, header=True, inferSchema=True)

   # Data cleaning: remove null age rows
    df = df.dropna(subset=["age"])

    # Transformations
    df = df.filter(col("age") > min_age)
    df = df.withColumn("salary_in_lakhs", col("salary") / 100000)

    # Write output
    df.write.mode("overwrite").csv(output_path, header=True)

    print("✅ Config-driven job completed")

    spark.stop()

if __name__ == "__main__":
    main()
