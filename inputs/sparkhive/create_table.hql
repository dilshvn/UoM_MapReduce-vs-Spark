CREATE TABLE IF NOT EXISTS delay_flights (
    Year INT,
    Month INT,
    DayofMonth INT,
    DayOfWeek INT,
    DepTime STRING,
    CRSDepTime STRING,
    ArrTime STRING,
    CRSArrTime STRING,
    UniqueCarrier STRING,
    FlightNum STRING,
    TailNum STRING,
    ActualElapsedTime STRING,
    CRSElapsedTime STRING,
    AirTime STRING,
    ArrDelay STRING,
    DepDelay STRING,
    Origin STRING,
    Dest STRING,
    Distance STRING,
    TaxiIn STRING,
    TaxiOut STRING,
    Cancelled STRING,
    CancellationCode STRING,
    Diverted STRING,
    CarrierDelay STRING,
    WeatherDelay STRING,
    NASDelay STRING,
    SecurityDelay STRING,
    LateAircraftDelay STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;