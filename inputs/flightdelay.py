from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("DelayAnalysis") \
    .getOrCreate()

df = spark.read.csv("s3://bigdataproject-bucket/project/DelayedFlights-updated.csv", header=True)

df.createOrReplaceTempView("delay_flights")

# Year wise carrier delay from 2003-2010
carrier_delay_result = spark.sql("""
    SELECT Year, AVG((CarrierDelay / ArrDelay) * 100) AS AvgCarrierDelayPercentage
    FROM delay_flights
    WHERE Year BETWEEN 2003 AND 2010
    GROUP BY Year
    ORDER BY Year
""")

carrier_delay_result.write.csv("s3://bigdataproject-bucket/output/carrier_delay_analysis_result.csv", header=True, mode='overwrite')

# Year wise NAS delay from 2003-2010
nas_delay_result = spark.sql("""
    SELECT Year, AVG((NASDelay / ArrDelay) * 100) AS AvgNASDelayPercentage
    FROM delay_flights
    WHERE Year BETWEEN 2003 AND 2010
    GROUP BY Year
    ORDER BY Year
""")

nas_delay_result.write.csv("s3://bigdataproject-bucket/output/nas_delay_analysis_result.csv", header=True, mode='overwrite')

# Year wise Weather delay from 2003-2010
weather_delay_result = spark.sql("""
    SELECT Year, AVG((WeatherDelay / ArrDelay) * 100) AS AvgWeatherDelayPercentage
    FROM delay_flights
    WHERE Year BETWEEN 2003 AND 2010
    GROUP BY Year
    ORDER BY Year
""")

weather_delay_result.write.csv("s3://bigdataproject-bucket/output/weather_delay_analysis_result.csv", header=True, mode='overwrite')

# Year wise late aircraft delay from 2003-2010
late_aircraft_delay_result = spark.sql("""
    SELECT Year, AVG((LateAircraftDelay / ArrDelay) * 100) AS AvgLateAircraftDelayPercentage
    FROM delay_flights
    WHERE Year BETWEEN 2003 AND 2010
    GROUP BY Year
    ORDER BY Year
""")

late_aircraft_delay_result.write.csv("s3://bigdataproject-bucket/output/late_aircraft_delay_analysis_result.csv", header=True, mode='overwrite')

# Year wise security delay from 2003-2010
security_delay_result = spark.sql("""
    SELECT Year, AVG((SecurityDelay / ArrDelay) * 100) AS AvgSecurityDelayPercentage
    FROM delay_flights
    WHERE Year BETWEEN 2003 AND 2010
    GROUP BY Year
    ORDER BY Year
""")

security_delay_result.write.csv("s3://bigdataproject-bucket/output/security_delay_analysis_result.csv", header=True, mode='overwrite')

spark.stop()