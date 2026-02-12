
CREATE OR REPLACE MATERIALIZED VIEW daily_readings_expanded AS
SELECT 
  t.name AS tenant,
  l.name AS location,
  dt.name AS device,
  r.min_value,
  r.max_value,
  r.avg_value,
  r.reading_date
FROM daily_readings r
JOIN trial.bronze.sensors s
  ON upper(r.SensorId) = upper(s.SensorId)
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
WHERE CAST(r.reading_date AS TIMESTAMP) >= (current_timestamp() - INTERVAL 2 MONTH)
GROUP BY r.reading_date, l.Name, t.Name, dt.Name, r.min_value, r.max_value, r.avg_value
ORDER BY r.reading_date;
