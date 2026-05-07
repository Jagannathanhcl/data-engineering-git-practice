from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def main():
    spark = SparkSession.builder \
        .appName("GitPracticeSparkJob") \
        .getOrCreate()

    df = spark.read.csv("data/raw_data.csv", header=True, inferSchema=True)

    df = df.filter(col("age") > 25)
    df = df.withColumn("salary_in_lakhs", col("salary") / 100000)

    df.write.mode("overwrite").csv("output/processed_data", header=True)

    print("Job completed")

    spark.stop()

if __name__ == "__main__":
    main()
