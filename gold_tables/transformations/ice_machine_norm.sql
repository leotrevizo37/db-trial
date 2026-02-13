
CREATE OR REPLACE MATERIALIZED VIEW ice_machine_norm 
AS SELECT
    DeviceId,
    percentile_approx((Ia+Ib+Ic), 0.10) AS p10_total,
    percentile_approx((Ia+Ib+Ic), 0.90) AS p90_total
FROM STREAM(trial.silver.ice_features_5m)
WHERE ts_5m >= current_timestamp() - INTERVAL 7 DAYS
GROUP BY DeviceId;
