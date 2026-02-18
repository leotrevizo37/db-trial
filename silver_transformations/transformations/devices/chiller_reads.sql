CREATE OR REPLACE STREAMING TABLE chiller_reads
WITH base AS (
  SELECT *
  FROM STREAM(trial.silver.reads)
  WHERE _device_type IN (
    'Cuarto de conservación',
    'Ambiente conservacion',
    'Compresor conservación'
  )
)
SELECT *
FROM base;
