
CREATE OR REPLACE VIEW gold.v_hourly_device_health AS
SELECT
    h.reading_hour,
    a.value_kind,
    t.Name AS tenant,
    t.tenantId,
    l.Name AS location,
    dt.Name AS device_type,
    d.Name AS device,
    st.Name AS sensor_type,
    h.min_value,
    h.max_value,
    h.avg_value,
    h.stddev_value,
    a.availability_pct,
    a.gap_minutes,
    s.SensorId,
    d.DeviceId
FROM hourly_reads h
JOIN trial.bronze.sensors s
    ON upper(h.SensorId) = upper(s.SensorId)
JOIN trial.bronze.sensor_tys st
    ON upper(s.SensorTyId) = upper(st.SensorTyId)
JOIN trial.bronze.devices d
    ON upper(s.DeviceId) = upper(d.DeviceId)
JOIN trial.bronze.device_tys dt
    ON upper(d.DeviceTyId) = upper(dt.DeviceTyId)
JOIN trial.bronze.sublocations sl
    ON upper(d.SublocationId) = upper(sl.SublocationId)
JOIN trial.bronze.locations l
    ON upper(sl.LocationId) = upper(l.LocationId)
JOIN trial.bronze.tenants t
    ON upper(l.TenantId) = upper(t.TenantId)
LEFT JOIN sensor_hourly_availability a
    ON upper(a.SensorId) = upper(h.SensorId)
    AND upper(a.DeviceId) = upper(d.DeviceId)
    AND a.reading_hour = h.reading_hour
WHERE h.reading_hour >= date_trunc('hour', current_timestamp() - INTERVAL 30 DAYS);
