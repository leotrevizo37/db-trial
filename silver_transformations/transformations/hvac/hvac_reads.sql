
CREATE OR REPLACE STREAMING TABLE hvac_reads AS
SELECT *
FROM STREAM(trial.silver.reads) AS r
WHERE r.DeviceId IN (SELECT DeviceId FROM trial.silver.dims_hvac_schedule);
