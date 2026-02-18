
CREATE OR REPLACE STREAMING TABLE hvac_reads
WITH base AS (
  SELECT *
  FROM STREAM(trial.silver.reads)
  WHERE _device_type IN (
    'Ambiente playland',
    'A/C de Comedor',
    'A/C de Cocina'
  )
)
SELECT *
FROM base;
