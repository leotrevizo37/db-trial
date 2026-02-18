CREATE OR REPLACE STREAMING TABLE vend_machine_reads
WITH base AS (
  SELECT *
  FROM STREAM(trial.silver.reads)
  WHERE _device_type IN (
    'Máquina de nieve',
    'Máquina de sodas'
  )
)
SELECT *
FROM base;
