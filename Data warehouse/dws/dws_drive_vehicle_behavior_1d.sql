CREATE TABLE IF NOT EXISTS dws_drive_vehicle_behavior_1d (
    vin                         STRING      COMMENT 'VIN码',
    vehicle_sk                  BIGINT      COMMENT '车辆代理键',
    stat_date                   STRING      COMMENT '统计日期',
    total_risk_event_cnt        INT         COMMENT '总风险事件次数',
    hard_brake_cnt              INT         COMMENT '急刹车次数',
    hard_accel_cnt              INT         COMMENT '急加速次数',
    sharp_turn_cnt              INT         COMMENT '急转弯次数',
    speeding_cnt                INT         COMMENT '超速次数',
    fatigue_driving_cnt         INT         COMMENT '疲劳驾驶次数',
    unbuckled_cnt               INT         COMMENT '未系安全带次数',
    high_risk_cnt               INT         COMMENT '高风险事件次数',
    medium_risk_cnt             INT         COMMENT '中风险事件次数',
    low_risk_cnt                INT         COMMENT '低风险事件次数',
    max_g_value                 DOUBLE      COMMENT '最大G值',
    max_acceleration            DOUBLE      COMMENT '最大加速度（m/s²）',
    max_speed_value             DOUBLE      COMMENT '风险事件最高车速',
    driving_score               DOUBLE      COMMENT '驾驶行为评分（0-100分）',
    risk_event_per_100km        DOUBLE      COMMENT '百公里风险事件次数'
)
COMMENT '车辆驾驶行为统计日表（DWS层）'
PARTITIONED BY (dt STRING)
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY');


INSERT OVERWRITE TABLE dws_drive_vehicle_behavior_1d PARTITION(dt)
SELECT
    vin,
    MAX(vehicle_sk) AS vehicle_sk,
    event_date AS stat_date,
    COUNT(*) AS total_risk_event_cnt,
    SUM(CASE WHEN event_type = '急刹车' THEN 1 ELSE 0 END) AS hard_brake_cnt,
    SUM(CASE WHEN event_type = '急加速' THEN 1 ELSE 0 END) AS hard_accel_cnt,
    SUM(CASE WHEN event_type = '急转弯' THEN 1 ELSE 0 END) AS sharp_turn_cnt,
    SUM(CASE WHEN event_type = '超速' THEN 1 ELSE 0 END) AS speeding_cnt,
    SUM(CASE WHEN event_type = '疲劳驾驶' THEN 1 ELSE 0 END) AS fatigue_driving_cnt,
    SUM(CASE WHEN event_type = '未系安全带' THEN 1 ELSE 0 END) AS unbuckled_cnt,
    SUM(CASE WHEN risk_level = 'HIGH' THEN 1 ELSE 0 END) AS high_risk_cnt,
    SUM(CASE WHEN risk_level = 'MEDIUM' THEN 1 ELSE 0 END) AS medium_risk_cnt,
    SUM(CASE WHEN risk_level = 'LOW' THEN 1 ELSE 0 END) AS low_risk_cnt,
    MAX(g_value) AS max_g_value,
    MAX(acceleration_g) AS max_acceleration,
    MAX(max_speed) AS max_speed_value,
    100 - (
        SUM(CASE WHEN event_type = '急刹车' THEN 2 ELSE 0 END) +
        SUM(CASE WHEN event_type = '急加速' THEN 1.5 ELSE 0 END) +
        SUM(CASE WHEN event_type = '超速' THEN 3 ELSE 0 END) +
        SUM(CASE WHEN event_type = '疲劳驾驶' THEN 5 ELSE 0 END) +
        SUM(CASE WHEN event_type = '未系安全带' THEN 1 ELSE 0 END)
    ) AS driving_score,
    CAST(COUNT(*) AS DOUBLE) / NULLIF(MAX(r.total_mileage), 0) * 100 AS risk_event_per_100km,
    event_date AS dt
FROM 
    dwd_drive_behavior_event_inc e
LEFT JOIN
    dws_drive_vehicle_running_1d r
    ON e.vin = r.vin AND e.event_date = r.stat_date AND r.dt = '${hiveconf:etl_date}'
WHERE 
    e.dt = '${hiveconf:etl_date}'
GROUP BY 
    vin, event_date;
