
CREATE OR REPLACE TEMPORARY VIEW hourly_readings AS
SELECT
    date_trunc('HOUR', LocalTimeSpan) AS reading_hour,
    MIN(Value) AS min_value,
    MAX(Value) AS max_value,
    AVG(Value) AS avg_value,
    SensorId
FROM TRIAL.bronze.reads_raw
GROUP BY date_trunc('HOUR', LocalTimeSpan), SensorId;
