CREATE OR REPLACE STREAMING TABLE freezer_reads
WITH base AS (
  SELECT *
  FROM STREAM(trial.silver.reads)
  WHERE _device_type IN (
    'Cuarto de congelación',
    'Ambiente congelacion',
    'Compresor congelación',
    'Resitencia de deshielo',
    'Resistencia de deshielo'
  )
)
SELECT *
FROM base;
