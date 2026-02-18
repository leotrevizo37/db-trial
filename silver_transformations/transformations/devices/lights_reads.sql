CREATE OR REPLACE STREAMING TABLE lights_reads
WITH base AS (
  SELECT *
  FROM STREAM(trial.silver.reads)
  WHERE _device_type IN (
    'Luminaria'
  )
)
SELECT *
FROM base;
