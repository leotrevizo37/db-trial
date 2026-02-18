
CREATE OR REPLACE STREAMING TABLE reads AS
SELECT
    r.ReadId,
    s.SensorId,
    d.DeviceId,
    dt.DeviceTyId,
    s.SensorTyId,
    r.LocalTimeSpan,
    r.Value,
    upper(split(trim(st.Name), ' ')[0]) AS value_kind,
    t.TenantId,
    sl.SublocationId,
    l.LocationId,
    dt.Name as _device_type
FROM STREAM(trial.bronze.reads_raw) AS r
JOIN trial.bronze.sensors s
    ON upper(r.SensorId) = upper(s.SensorId)
JOIN trial.bronze.devices d
    ON upper(s.DeviceId) = upper(d.DeviceId)
JOIN trial.bronze.sensor_tys st
    ON upper(s.SensorTyId) = upper(st.SensorTyId)
JOIN trial.bronze.device_tys dt
    ON upper(d.DeviceTyId) = upper(dt.DeviceTyId)
JOIN trial.bronze.sublocations sl
    ON upper(d.SublocationId) = upper(sl.SublocationId)
JOIN trial.bronze.locations l
    ON upper(sl.LocationId) = upper(l.LocationId)
JOIN trial.bronze.tenants t
    ON upper(l.TenantId) = upper(t.TenantId);
