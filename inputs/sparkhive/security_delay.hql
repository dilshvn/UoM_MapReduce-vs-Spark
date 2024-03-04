CREATE TABLE IF NOT EXISTS security_delay_result AS
SELECT Year, AVG((SecurityDelay / ArrDelay) * 100) AS AvgSecurityDelayPercentage
FROM delay_flights
WHERE Year BETWEEN 2003 AND 2010
GROUP BY Year
ORDER BY Year;