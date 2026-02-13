
CREATE OR REPLACE STREAMING TABLE
    ice_machines_sensors
AS SELECT
    d.DeviceId,
    s.SensorId,
    CASE
        WHEN lower(s.Name) LIKE '%fase 1%' THEN 1
        WHEN lower(s.Name) LIKE '%fase 2%' THEN 2
        WHEN lower(s.Name) LIKE '%fase 3%' THEN 3
        ELSE NULL   
    END AS phase
FROM STREAM(trial.silver.reads) AS r
JOIN trial.bronze.sensors s
    ON upper(r.SensorId) = upper(s.SensorId)
JOIN trial.bronze.devices d
    ON upper(s.DeviceId) = upper(d.DeviceId)
JOIN trial.bronze.sensor_tys st
    ON upper(s.SensorTyId) = upper(st.SensorTyId)
JOIN trial.bronze.device_tys dt
WHERE lower(dt.Name) LIKE '%nieve%'
    AND lower(st.Name) LIKE '%corriente%'
    AND r.LocalTimeSpan >= current_timestamp() - INTERVAL 3 month;
