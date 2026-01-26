CREATE TABLE IF NOT EXISTS dws_drive_vehicle_running_30d (
    vin                         STRING      COMMENT 'VIN码',
    vehicle_sk                  BIGINT      COMMENT '车辆代理键',
    model_name                  STRING      COMMENT '车型名称',
    stat_date                   STRING      COMMENT '统计日期（窗口结束日期）',
    total_mileage_30d           DOUBLE      COMMENT '30日累计里程',
    total_driving_duration_min  INT         COMMENT '30日累计行驶时长',
    active_days                 INT         COMMENT '活跃天数',
    avg_daily_mileage           DOUBLE      COMMENT '日均里程',
    total_energy_consumption    DOUBLE      COMMENT '30日总能耗',
    avg_energy_per_100km        DOUBLE      COMMENT '30日百公里能耗',
    total_risk_event_cnt_30d    INT         COMMENT '30日总风险事件数',
    avg_driving_score_30d       DOUBLE      COMMENT '30日平均驾驶评分',
    soh_start                   DOUBLE      COMMENT '期初SOH',
    soh_end                     DOUBLE      COMMENT '期末SOH',
    soh_degradation             DOUBLE      COMMENT 'SOH衰减量'
)
COMMENT '车辆行驶统计30日表（DWS层-滚动窗口）'
PARTITIONED BY (dt STRING COMMENT '统计日期')
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY');

INSERT OVERWRITE TABLE dws_drive_vehicle_running_30d PARTITION(dt)
SELECT
    vin,
    MAX(vehicle_sk) AS vehicle_sk,
    MAX(model_name) AS model_name,
    '${hiveconf:etl_date}' AS stat_date,
    SUM(total_mileage) AS total_mileage_30d,
    SUM(driving_duration_min) AS total_driving_duration_min,
    COUNT(DISTINCT stat_date) AS active_days,
    AVG(total_mileage) AS avg_daily_mileage,
    SUM(energy_consumption_kwh) AS total_energy_consumption,
    SUM(energy_consumption_kwh) / NULLIF(SUM(total_mileage), 0) * 100 AS avg_energy_per_100km,
    SUM(COALESCE(b.total_risk_event_cnt, 0)) AS total_risk_event_cnt_30d,
    AVG(COALESCE(b.driving_score, 100)) AS avg_driving_score_30d,
    FIRST_VALUE(avg_soh) OVER (PARTITION BY vin ORDER BY stat_date) AS soh_start,
    LAST_VALUE(avg_soh) OVER (PARTITION BY vin ORDER BY stat_date) AS soh_end,
    LAST_VALUE(avg_soh) OVER (...) - FIRST_VALUE(avg_soh) OVER (...) AS soh_degradation,
    '${hiveconf:etl_date}' AS dt
FROM 
    dws_drive_vehicle_running_1d r
LEFT JOIN
    dws_drive_vehicle_behavior_1d b
    ON r.vin = b.vin AND r.stat_date = b.stat_date AND b.dt BETWEEN DATE_SUB('${hiveconf:etl_date}', 30) AND '${hiveconf:etl_date}'
WHERE 
    r.dt BETWEEN DATE_SUB('${hiveconf:etl_date}', 30) AND '${hiveconf:etl_date}'
GROUP BY 
    vin;
