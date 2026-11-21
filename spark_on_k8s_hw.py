
import time
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("Spark on K8s HW").getOrCreate()
try:
    df = (spark.range(100000)
          .withColumn("plus_10", F.col("id") + 10)
          .withColumn("double_id", F.col("id") * 2))
    print("=== WEEK 9: FIRST 50 ROWS ===", flush=True)
    df.show(50, truncate=False)
    print("=== STUDENT: MURAD FARAJOV ===", flush=True)
    print("Sleeping for 30 seconds to allow screenshots of executor pods...", flush=True)
    time.sleep(30)
finally:
    print("Stopping SparkSession", flush=True)
    spark.stop()
