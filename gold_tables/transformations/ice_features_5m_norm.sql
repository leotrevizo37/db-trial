CREATE OR REPLACE MATERIALIZED VIEW gold.ice_features_5m_norm AS
SELECT
    f.*,
    (f.Ia + f.Ib + f.Ic) AS I_total,
    CASE
        WHEN (n.p90_total - n.p10_total) > 0 THEN ( (f.Ia+f.Ib+f.Ic) - n.p10_total ) / (n.p90_total - n.p10_total)
        ELSE 0
    END AS I_norm,
    CASE
        WHEN ( (f.Ia+f.Ib+f.Ic) / 3 ) > 0 THEN
            (sqrt( (pow(f.Ia - ((f.Ia+f.Ib+f.Ic)/3),2) + pow(f.Ib - ((f.Ia+f.Ib+f.Ic)/3),2) + pow(f.Ic - ((f.Ia+f.Ib+f.Ic)/3),2)) / 3 ))
            / (((f.Ia+f.Ib+f.Ic)/3) + 1e-6)
        ELSE 0
    END AS imbalance
FROM STREAM(trial.silver.ice_features_5m) f
JOIN trial.gold.ice_machine_norm n USING (DeviceId);
