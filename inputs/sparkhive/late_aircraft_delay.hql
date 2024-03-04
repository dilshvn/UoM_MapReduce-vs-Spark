CREATE TABLE IF NOT EXISTS late_aircraft_delay_result AS
SELECT Year, AVG((LateAircraftDelay / ArrDelay) * 100) AS AvgLateAircraftDelayPercentage
FROM delay_flights
WHERE Year BETWEEN 2003 AND 2010
GROUP BY Year
ORDER BY Year;