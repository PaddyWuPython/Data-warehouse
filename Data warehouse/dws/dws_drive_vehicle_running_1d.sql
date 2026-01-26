CREATE TABLE IF NOT EXISTS dws_drive_vehicle_running_1d (
    -- 维度字段
    vin                         STRING      COMMENT 'VIN码',
    vehicle_sk                  BIGINT      COMMENT '车辆代理键',
    model_name                  STRING      COMMENT '车型名称',
    battery_type                STRING      COMMENT '电池类型',
    province_code               STRING      COMMENT '归属省份',
    city_code                   STRING      COMMENT '归属城市',
    
    -- 时间维度
    stat_date                   STRING      COMMENT '统计日期',
    
    -- 基础运行指标
    total_mileage               DOUBLE      COMMENT '总里程（km）',
    driving_duration_min        INT         COMMENT '行驶时长（分钟）',
    online_duration_min         INT         COMMENT '在线时长（分钟）',
    trip_count                  INT         COMMENT '出行次数',
    avg_speed                   DOUBLE      COMMENT '平均车速（km/h）',
    max_speed                   DOUBLE      COMMENT '最高车速（km/h）',
    
    -- 能耗指标
    energy_consumption_kwh      DOUBLE      COMMENT '总能耗（kWh）',
    avg_energy_per_100km        DOUBLE      COMMENT '百公里能耗（kWh/100km）',
    avg_soc                     DOUBLE      COMMENT '平均SOC（%）',
    soc_consumption             DOUBLE      COMMENT 'SOC消耗量（%）',
    
    -- 工况指标
    pure_electric_mileage       DOUBLE      COMMENT '纯电里程（km）',
    hybrid_mileage              DOUBLE      COMMENT '混动里程（km）',
    fuel_mileage                DOUBLE      COMMENT '燃油里程（km）',
    
    -- 三电指标
    avg_voltage                 DOUBLE      COMMENT '平均总电压（V）',
    avg_current                 DOUBLE      COMMENT '平均总电流（A）',
    max_battery_temp            DOUBLE      COMMENT '最高电池温度（℃）',
    avg_battery_temp            DOUBLE      COMMENT '平均电池温度（℃）',
    max_motor_temp              DOUBLE      COMMENT '最高电机温度（℃）',
    
    -- 数据质量
    valid_record_cnt            INT         COMMENT '有效记录数',
    invalid_gps_cnt             INT         COMMENT 'GPS异常记录数',
    data_quality_score          DOUBLE      COMMENT '数据质量评分'
)
COMMENT '车辆行驶统计日表（DWS层）'
PARTITIONED BY (dt STRING COMMENT '统计日期 yyyy-MM-dd')
STORED AS ORC
TBLPROPERTIES (
    'orc.compress'='SNAPPY',
    'orc.stripe.size'='67108864'
);

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE dws_drive_vehicle_running_1d PARTITION(dt)
SELECT
    vin,
    MAX(vehicle_sk) AS vehicle_sk,
    MAX(model_name) AS model_name,
    MAX(battery_type) AS battery_type,
    MAX(province_code) AS province_code,
    MAX(city_code) AS city_code,
    collect_date AS stat_date,
    SUM(CASE WHEN speed > 0 THEN (odometer - LAG(odometer) OVER (PARTITION BY vin ORDER BY collect_time)) ELSE 0 END) AS total_mileage,
    SUM(CASE WHEN vehicle_status = 1 THEN 1 ELSE 0 END) / 2 AS driving_duration_min,
    COUNT(*) / 2 AS online_duration_min,
    COUNT(DISTINCT CONCAT(vin, trip_flag)) AS trip_count,
    AVG(CASE WHEN speed > 0 THEN speed ELSE NULL END) AS avg_speed,
    MAX(speed) AS max_speed,
    SUM(ABS(total_current) * total_voltage / 1000.0 * (30.0/3600)) AS energy_consumption_kwh,
    (SUM(ABS(total_current) * total_voltage / 1000.0 * (30.0/3600)) / NULLIF(SUM(...), 0)) * 100 AS avg_energy_per_100km,
    AVG(soc) AS avg_soc,
    MAX(soc) - MIN(soc) AS soc_consumption,
    SUM(CASE WHEN run_mode = 1 AND speed > 0 THEN (odometer - LAG(odometer) OVER (...)) ELSE 0 END) AS pure_electric_mileage,
    SUM(CASE WHEN run_mode = 2 AND speed > 0 THEN ... END) AS hybrid_mileage,
    SUM(CASE WHEN run_mode = 3 AND speed > 0 THEN ... END) AS fuel_mileage,
    AVG(total_voltage) AS avg_voltage,
    AVG(total_current) AS avg_current,
    MAX(main_motor_temp) AS max_battery_temp,
    AVG(main_motor_temp) AS avg_battery_temp,
    MAX(main_controller_temp) AS max_motor_temp,
    COUNT(*) AS valid_record_cnt,
    SUM(CASE WHEN is_valid_gps = 0 THEN 1 ELSE 0 END) AS invalid_gps_cnt,
    (1 - SUM(CASE WHEN is_valid_gps = 0 THEN 1 ELSE 0 END) / COUNT(*)) * 100 AS data_quality_score,
    collect_date AS dt
FROM 
    dwd_drive_running_log_inc
WHERE 
    dt = '${hiveconf:etl_date}'
GROUP BY 
    vin, collect_date;
