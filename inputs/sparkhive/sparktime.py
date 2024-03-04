import time
from pyspark.sql import SparkSession
import csv

start_time = time.time()

spark = SparkSession.builder \
    .appName("DelayAnalysis") \
    .getOrCreate()

df = spark.read.csv("s3://bigdataproject-bucket/project/DelayedFlights-updated.csv", header=True)
df.createOrReplaceTempView("delay_flights")

query_runtimes = []

query_start_time = time.time()
carrier_delay_result = spark.sql("""
    SELECT Year, AVG((CarrierDelay / ArrDelay) * 100) AS AvgCarrierDelayPercentage
    FROM delay_flights
    WHERE Year BETWEEN 2003 AND 2010
    GROUP BY Year
    ORDER BY Year
""")
query_end_time = time.time()
query_runtimes.append(query_end_time - query_start_time)

query_start_time = time.time()
nas_delay_result = spark.sql("""
    SELECT Year, AVG((NASDelay / ArrDelay) * 100) AS AvgNASDelayPercentage
    FROM delay_flights
    WHERE Year BETWEEN 2003 AND 2010
    GROUP BY Year
    ORDER BY Year
""")
query_end_time = time.time()
query_runtimes.append(query_end_time - query_start_time)

query_start_time = time.time()
weather_delay_result = spark.sql("""
    SELECT Year, AVG((WeatherDelay / ArrDelay) * 100) AS AvgWeatherDelayPercentage
    FROM delay_flights
    WHERE Year BETWEEN 2003 AND 2010
    GROUP BY Year
    ORDER BY Year
""")
query_end_time = time.time()
query_runtimes.append(query_end_time - query_start_time)

query_start_time = time.time()
late_aircraft_delay_result = spark.sql("""
    SELECT Year, AVG((LateAircraftDelay / ArrDelay) * 100) AS AvgLateAircraftDelayPercentage
    FROM delay_flights
    WHERE Year BETWEEN 2003 AND 2010
    GROUP BY Year
    ORDER BY Year
""")
query_end_time = time.time()
query_runtimes.append(query_end_time - query_start_time)

query_start_time = time.time()
security_delay_result = spark.sql("""
    SELECT Year, AVG((SecurityDelay / ArrDelay) * 100) AS AvgSecurityDelayPercentage
    FROM delay_flights
    WHERE Year BETWEEN 2003 AND 2010
    GROUP BY Year
    ORDER BY Year
""")
query_end_time = time.time()
query_runtimes.append(query_end_time - query_start_time)

with open('query_runtimes.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['Query', 'Runtime (seconds)'])
    writer.writerow(['Carrier Delay', query_runtimes[0]])
    writer.writerow(['NAS Delay', query_runtimes[1]])
    writer.writerow(['Weather Delay', query_runtimes[2]])
    writer.writerow(['Late Aircraft Delay', query_runtimes[3]])
    writer.writerow(['Security Delay', query_runtimes[4]])

spark.stop()

end_time = time.time()
total_runtime = end_time - start_time
print("Total runtime for all queries:", total_runtime, "seconds")
