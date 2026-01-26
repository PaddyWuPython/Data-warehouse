CREATE TABLE IF NOT EXISTS dws_chg_vehicle_charging_1d (
    vin                         STRING      COMMENT 'VIN码',
    vehicle_sk                  BIGINT      COMMENT '车辆代理键',
    stat_date                   STRING      COMMENT '统计日期',
    charge_session_cnt          INT         COMMENT '充电次数',
    completed_session_cnt       INT         COMMENT '完成充电次数',
    abnormal_session_cnt        INT         COMMENT '异常中断次数',
    abnormal_rate               DOUBLE      COMMENT '充电异常率（%）',
    total_charged_energy        DOUBLE      COMMENT '总充电电量（kWh）',
    total_charge_duration_min   INT         COMMENT '总充电时长（分钟）',
    avg_charged_energy          DOUBLE      COMMENT '平均单次充电量（kWh）',
    avg_charge_duration_min     INT         COMMENT '平均单次充电时长（分钟）',
    avg_soc_gain                DOUBLE      COMMENT '平均SOC增量（%）',
    avg_charging_power          DOUBLE      COMMENT '平均充电功率（kW）',
    max_charging_power          DOUBLE      COMMENT '最大充电功率（kW）',
    over_temp_cnt               INT         COMMENT '充电过温次数（>50℃）',
    over_current_cnt            INT         COMMENT '充电过流次数',
    max_temp_during_charge      DOUBLE      COMMENT '充电最高温度（℃）',
    fast_charge_cnt             INT         COMMENT '快充次数（功率>50kW）',
    slow_charge_cnt             INT         COMMENT '慢充次数',
    fast_charge_ratio           DOUBLE      COMMENT '快充占比（%）'
)
COMMENT '车辆充电统计日表（DWS层）'
PARTITIONED BY (dt STRING)
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY');

INSERT OVERWRITE TABLE dws_chg_vehicle_charging_1d PARTITION(dt)
SELECT
    s.vin,
    MAX(s.vehicle_sk) AS vehicle_sk,
    DATE(s.plug_in_time) AS stat_date,
    COUNT(*) AS charge_session_cnt,
    SUM(CASE WHEN s.session_status = 'COMPLETED' THEN 1 ELSE 0 END) AS completed_session_cnt,
    SUM(s.is_abnormal_stop) AS abnormal_session_cnt,
    SUM(s.is_abnormal_stop) / COUNT(*) * 100 AS abnormal_rate,
    SUM(s.charged_energy) AS total_charged_energy,
    SUM(s.charge_duration_min) AS total_charge_duration_min,
    AVG(s.charged_energy) AS avg_charged_energy,
    AVG(s.charge_duration_min) AS avg_charge_duration_min,
    AVG(s.soc_gain) AS avg_soc_gain,
    AVG(s.avg_power) AS avg_charging_power,
    MAX(s.max_power) AS max_charging_power,
    SUM(CASE WHEN m.max_cell_temp > 50 THEN 1 ELSE 0 END) AS over_temp_cnt,
    SUM(CASE WHEN m.is_over_current = 1 THEN 1 ELSE 0 END) AS over_current_cnt,
    MAX(m.max_cell_temp) AS max_temp_during_charge,
    SUM(CASE WHEN s.avg_power > 50 THEN 1 ELSE 0 END) AS fast_charge_cnt,
    SUM(CASE WHEN s.avg_power <= 50 THEN 1 ELSE 0 END) AS slow_charge_cnt,
    SUM(CASE WHEN s.avg_power > 50 THEN 1 ELSE 0 END) / COUNT(*) * 100 AS fast_charge_ratio,
    DATE(s.plug_in_time) AS dt
FROM 
    dwd_chg_session_acc s
LEFT JOIN
    dwd_chg_monitor_log_inc m
    ON s.vin = m.vin AND s.session_id = m.session_id AND m.dt = '${hiveconf:etl_date}'
WHERE 
    s.dt = '${hiveconf:etl_date}'
GROUP BY 
    s.vin, DATE(s.plug_in_time);
