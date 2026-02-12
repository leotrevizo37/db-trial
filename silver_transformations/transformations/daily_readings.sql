
CREATE OR REPLACE MATERIALIZED VIEW daily_readings AS
SELECT
    date(reading_hour) AS reading_date,
    MIN(avg_value) AS min_value,
    MAX(avg_value) AS max_value,
    AVG(avg_value) AS avg_value,
    SensorId
FROM hourly_readings
GROUP BY date(reading_hour), SensorId;
