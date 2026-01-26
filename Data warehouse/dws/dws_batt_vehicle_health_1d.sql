CREATE TABLE IF NOT EXISTS dws_batt_vehicle_health_1d (
    vin                         STRING      COMMENT 'VIN码',
    vehicle_sk                  BIGINT      COMMENT '车辆代理键',
    battery_pack_code           STRING      COMMENT '电池包编码',
    battery_type                STRING      COMMENT '电池类型',
    stat_date                   STRING      COMMENT '统计日期',
    avg_pack_voltage            DOUBLE      COMMENT '平均总电压（V）',
    avg_voltage_diff            DOUBLE      COMMENT '平均压差（V）',
    max_voltage_diff            DOUBLE      COMMENT '最大压差（V）',
    voltage_abnormal_cnt        INT         COMMENT '电压异常次数',
    voltage_abnormal_duration_min INT       COMMENT '电压异常持续时长（分钟）',
    avg_max_temp                DOUBLE      COMMENT '平均最高温度（℃）',
    max_temp_value              DOUBLE      COMMENT '当日最高温度（℃）',
    avg_temp_diff               DOUBLE      COMMENT '平均温差（℃）',
    max_temp_diff               DOUBLE      COMMENT '最大温差（℃）',
    temp_abnormal_cnt           INT         COMMENT '温度异常次数',
    over_temp_cnt               INT         COMMENT '过温次数（>55℃）',
    avg_soc                     DOUBLE      COMMENT '平均SOC（%）',
    min_soc                     DOUBLE      COMMENT '最低SOC',
    avg_soh                     DOUBLE      COMMENT '平均SOH（%）',
    soh_degradation_rate        DOUBLE      COMMENT 'SOH日衰减率（‰）',
    consistency_score           DOUBLE      COMMENT '一致性评分（0-100）',
    is_thermal_risk             TINYINT     COMMENT '是否存在热失控风险',
    is_consistency_risk         TINYINT     COMMENT '是否存在一致性风险',
    sample_cnt                  INT         COMMENT '采样次数'
)
COMMENT '车辆电池健康统计日表（DWS层）'
PARTITIONED BY (dt STRING)
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY');

INSERT OVERWRITE TABLE dws_batt_vehicle_health_1d PARTITION(dt)
SELECT
    c.vin,
    MAX(c.vehicle_sk) AS vehicle_sk,
    MAX(c.battery_pack_code) AS battery_pack_code,
    MAX(c.battery_type) AS battery_type,
    c.collect_date AS stat_date,
    AVG(c.pack_voltage) AS avg_pack_voltage,
    AVG(c.voltage_diff) AS avg_voltage_diff,
    MAX(c.voltage_diff) AS max_voltage_diff,
    SUM(c.is_voltage_abnormal) AS voltage_abnormal_cnt,
    SUM(CASE WHEN c.is_voltage_abnormal = 1 THEN 1 ELSE 0 END) / 2 AS voltage_abnormal_duration_min,
    AVG(c.max_cell_temp) AS avg_max_temp,
    MAX(c.max_cell_temp) AS max_temp_value,
    AVG(c.temp_diff) AS avg_temp_diff,
    MAX(c.temp_diff) AS max_temp_diff,
    SUM(c.is_temp_abnormal) AS temp_abnormal_cnt,
    SUM(CASE WHEN c.max_cell_temp > 55 THEN 1 ELSE 0 END) AS over_temp_cnt,
    AVG(c.soc) AS avg_soc,
    MIN(c.soc) AS min_soc,
    AVG(c.soh) AS avg_soh,
    (FIRST_VALUE(c.soh) OVER (PARTITION BY c.vin ORDER BY c.collect_time) - 
     LAST_VALUE(c.soh) OVER (PARTITION BY c.vin ORDER BY c.collect_time)) * 1000 AS soh_degradation_rate,
    100 - (AVG(c.voltage_diff) / 0.2 * 30 + AVG(c.temp_diff) / 20 * 20) AS consistency_score,
    MAX(CASE WHEN e.is_thermal_risk = 1 THEN 1 ELSE 0 END) AS is_thermal_risk,
    MAX(CASE WHEN e.is_consistency_risk = 1 THEN 1 ELSE 0 END) AS is_consistency_risk,
    COUNT(*) AS sample_cnt,
    c.collect_date AS dt
FROM 
    dwd_batt_cell_log_inc c
LEFT JOIN
    dwd_batt_extreme_log_inc e
    ON c.vin = e.vin AND c.collect_time = e.collect_time AND e.dt = '${hiveconf:etl_date}'
WHERE 
    c.dt = '${hiveconf:etl_date}'
GROUP BY 
    c.vin, c.collect_date;
