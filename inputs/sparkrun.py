from pyspark.sql import SparkSession
import time

spark = SparkSession.builder \
    .appName("DelayAnalysisBenchmark") \
    .getOrCreate()

df = spark.read.csv("s3://bigdataproject-bucket/project/DelayedFlights-updated.csv", header=True)
df.createOrReplaceTempView("delay_flights")

query = """
    SELECT Year, AVG((CarrierDelay / ArrDelay) * 100) AS AvgCarrierDelayPercentage
    FROM delay_flights
    WHERE Year BETWEEN 2003 AND 2010
    GROUP BY Year
    ORDER BY Year
"""

running_times = []

for i in range(5):
    start_time = time.time()
    result_df = spark.sql(query)
    result_df.collect()
    end_time = time.time()
    running_time = end_time - start_time
    running_times.append(running_time)

running_times_df = spark.createDataFrame([(i+1, time_val) for i, time_val in enumerate(running_times)], ["Iteration", "RunningTime"])
running_times_df.write.csv("s3://bigdataproject-bucket/output/carrier_delay_running_times.csv", header=True, mode="overwrite")

spark.stop()