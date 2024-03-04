INSERT OVERWRITE LOCAL DIRECTORY '/tmp/query_runtimes'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT 'Carrier Delay', CAST(UNIX_TIMESTAMP() AS STRING), carrier_delay_runtime FROM carrier_delay_result
UNION ALL
SELECT 'NAS Delay', CAST(UNIX_TIMESTAMP() AS STRING), nas_delay_runtime FROM nas_delay_result
UNION ALL
SELECT 'Weather Delay', CAST(UNIX_TIMESTAMP() AS STRING), weather_delay_runtime FROM weather_delay_result
UNION ALL
SELECT 'Late Aircraft Delay', CAST(UNIX_TIMESTAMP() AS STRING), late_aircraft_delay_runtime FROM late_aircraft_delay_result
UNION ALL
SELECT 'Security Delay', CAST(UNIX_TIMESTAMP() AS STRING), security_delay_runtime FROM security_delay_result;