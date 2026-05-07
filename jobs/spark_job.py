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

    input_path = config["input_path"]
    output_path = config["output_path"]
    min_age = config["min_age"]

    # Read data
    df = spark.read.csv(input_path, header=True, inferSchema=True)

    # Data validation
    if df.filter(df["age"].isNull()).count() > 0:
    raise Exception("Data validation failed: Null values found in age column")

    # Transformations
    df = df.filter(col("age") > min_age)
    df = df.withColumn("salary_in_lakhs", col("salary") / 100000)

    # Write output
    df.write.mode("overwrite").csv(output_path, header=True)

    print("✅ Config-driven job completed")

    spark.stop()

if __name__ == "__main__":
    main()
