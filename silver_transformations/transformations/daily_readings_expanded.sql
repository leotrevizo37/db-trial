CREATE OR REPLACE MATERIALIZED VIEW daily_readings_expanded AS
SELECT 
  r.reading_date,
  r.min_value,
  r.max_value,
  r.avg_value,
  r.SensorId,
  s.SensorTyId,
  s.DeviceId,
  s.ModelId,
  s.SensorHubId,
  s.Name as SensorName,
  s.Active as SensorActive,
  s.PhysicalIdentifier,
  s.CreatedAt as SensorCreatedAt,
  s.ModifiedAt as SensorModifiedAt,
  s.Topic,
  s.FieldName,
  s.PerEvent,
  s.Conversion,
  s.ExtraValue1,
  s.ExtraValue2,
  s.ExtraValue3,
  s.ExtraValue4,
  d.DeviceTyId,
  d.SubLocationId,
  d.Name as DeviceName,
  d.Active as DeviceActive,
  d.ModelId as DeviceModelId,
  d.CreatedAt as DeviceCreatedAt,
  d.ModifiedAt as DeviceModifiedAt,
  d.Image,
  dt.Name as DeviceTypeName,
  dt.Active as DeviceTypeActive,
  dt.CreatedAt as DeviceTypeCreatedAt,
  dt.ModifiedAt as DeviceTypeModifiedAt,
  sl.LocationId,
  sl.SubLocationParentId,
  sl.SubLocationTyId,
  sl.Name as SubLocationName,
  sl.Active as SubLocationActive,
  sl.CreatedAt as SubLocationCreatedAt,
  sl.ModifiedAt as SubLocationModifiedAt,
  sl.Latitude,
  sl.Longitude,
  sl.Point,
  l.CityId,
  l.Name as LocationName,
  l.BusinessTyId,
  l.RegionId,
  l.IdentifierCode,
  l.Active as LocationActive,
  l.CreatedAt as LocationCreatedAt,
  l.ModifiedAt as LocationModifiedAt,
  l.LocationTyId,
  l.TenantId,
  t.Name as TenantName
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
ORDER BY r.reading_date;
