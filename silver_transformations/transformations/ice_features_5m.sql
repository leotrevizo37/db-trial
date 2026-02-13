CREATE OR REPLACE STREAMING TABLE ice_features_5m AS
SELECT
    m.DeviceId,
    r.LocalTimespan AS ts_5m,
    MAX(CASE WHEN m.phase = 1 THEN r.Value END) AS Ia,
    MAX(CASE WHEN m.phase = 2 THEN r.Value END) AS Ib,
    MAX(CASE WHEN m.phase = 3 THEN r.Value END) AS Ic
FROM (
    SELECT *
    FROM STREAM(trial.silver.reads)
    WATERMARK LocalTimeSpan DELAY OF INTERVAL 2 HOURS
    WHERE LocalTimeSpan >= current_timestamp() - INTERVAL 50 DAYS
) r
JOIN trial.silver.ice_machines_sensors m
    ON r.SensorId = m.SensorId
GROUP BY
    m.DeviceId,
    r.LocalTimeSpan;
