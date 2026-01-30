from pyspark.sql import SparkSession
import sys

def run(env):
    spark = SparkSession.builder.getOrCreate()

    data = [("Alice", 10), ("Bob", 20)]
    df = spark.createDataFrame(data, ["name", "value"])

    output_path = f"dbfs:/tmp/{env}/output"

    df.write.mode("overwrite").parquet(output_path)

    print(f"Data written to {output_path}")

if __name__ == "__main__":
    env = sys.argv[1]
    run(env)
