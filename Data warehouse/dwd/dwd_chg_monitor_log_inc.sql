DROP TABLE IF EXISTS dwd_chg_monitor_log_inc;

CREATE TABLE IF NOT EXISTS dwd_chg_monitor_log_inc (
    -- 维度字段
    vin                         STRING      COMMENT 'VIN码',
    vehicle_sk                  BIGINT      COMMENT '车辆代理键',
    session_id                  STRING      COMMENT '充电会话ID（关联充电订单）',
    charger_id                  STRING      COMMENT '充电桩ID',
    station_id                  STRING      COMMENT '充电站ID',
    station_name                STRING      COMMENT '充电站名称（退化维度）',
    
    -- 时间字段
    collect_time                TIMESTAMP   COMMENT '采集时间',
    collect_date                STRING      COMMENT '采集日期',
    
    -- 充电过程度量
    charging_voltage            DOUBLE      COMMENT '充电电压（V）',
    charging_current            DOUBLE      COMMENT '充电电流（A）',
    charging_power              DOUBLE      COMMENT '充电功率（kW）= 电压 * 电流 / 1000',
    soc                         DOUBLE      COMMENT '当前SOC（%）',
    pack_voltage                DOUBLE      COMMENT '电池包电压（V）',
    pack_current                DOUBLE      COMMENT '电池包电流（A）',
    
    -- 温度监控
    max_cell_temp               DOUBLE      COMMENT '充电时最高单体温度（℃）',
    avg_cell_temp               DOUBLE      COMMENT '充电时平均温度（℃）',
    
    -- 充电状态
    bms_charge_status           TINYINT     COMMENT 'BMS充电状态：1-充电中 2-已完成 3-异常',
    charge_status               TINYINT     COMMENT '车辆充电状态',
    
    -- 地理位置
    lat                         DOUBLE      COMMENT '纬度',
    lon                         DOUBLE      COMMENT '经度',
    
    -- 风险监控
    is_over_temp                TINYINT     COMMENT '过温风险：1-温度>50℃ 0-正常',
    is_over_current             TINYINT     COMMENT '过流风险：1-电流超额定值 0-正常',
    
    -- ETL元数据
    etl_date                    STRING      COMMENT 'ETL处理日期',
    create_time                 TIMESTAMP   COMMENT '记录创建时间'
)
COMMENT '充电过程监控明细表（事务型事实表）'
PARTITIONED BY (dt STRING COMMENT '分区字段：采集日期 yyyy-MM-dd')
STORED AS ORC
TBLPROPERTIES (
    'orc.compress'='SNAPPY'
);

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE dwd_chg_monitor_log_inc PARTITION(dt)
SELECT
    -- 维度字段
    t.vin,
    COALESCE(v.vehicle_sk, -1) AS vehicle_sk,
    NULL AS session_id,  -- 需要通过关联充电订单表获取
    t.charger_id,
    t.station_id,
    s.station_name,
    
    -- 时间字段
    t.collect_time,
    FROM_UNIXTIME(UNIX_TIMESTAMP(t.collect_time), 'yyyy-MM-dd') AS collect_date,
    
    -- 充电度量
    t.total_voltage / 10.0 AS charging_voltage,
    (t.total_current - 10000) / 10.0 AS charging_current,
    (t.total_voltage / 10.0) * ABS((t.total_current - 10000) / 10.0) / 1000.0 AS charging_power,
    t.soc / 10.0 AS soc,
    t.pack_voltage / 10.0 AS pack_voltage,
    (t.pack_current - 10000) / 10.0 AS pack_current,
    
    -- 温度（从电池数组提取）
    array_max(TRANSFORM(t.probe_temp_list, x -> x - 40.0)) AS max_cell_temp,
    (aggregate(TRANSFORM(t.probe_temp_list, x -> x - 40.0), 
        CAST(0.0 AS DOUBLE), (acc, x) -> acc + x) / SIZE(t.probe_temp_list)) AS avg_cell_temp,
    
    -- 状态
    t.bms_charge_status,
    t.charge_status,
    
    -- 位置
    t.lat / 1000000.0 AS lat,
    t.lon / 1000000.0 AS lon,
    
    -- 风险标识
    CASE 
        WHEN array_max(TRANSFORM(t.probe_temp_list, x -> x - 40.0)) > 50 THEN 1
        ELSE 0
    END AS is_over_temp,
    
    CASE 
        WHEN ABS((t.total_current - 10000) / 10.0) > 200 THEN 1  -- 假设额定电流200A
        ELSE 0
    END AS is_over_current,
    
    -- ETL元数据
    '${hiveconf:etl_date}' AS etl_date,
    CURRENT_TIMESTAMP() AS create_time,
    
    -- 动态分区
    FROM_UNIXTIME(UNIX_TIMESTAMP(t.collect_time), 'yyyy-MM-dd') AS dt
    
FROM 
    ods_log_vehicle_track_inc t
LEFT JOIN 
    dim_vehicle_zip v 
    ON t.vin = v.vin 
    AND FROM_UNIXTIME(UNIX_TIMESTAMP(t.collect_time), 'yyyy-MM-dd') BETWEEN v.start_date AND v.end_date
LEFT JOIN
    ods_biz_charging_station_full s
    ON t.station_id = s.station_id
    AND s.dt = '${hiveconf:etl_date}'
WHERE 
    t.dt = '${hiveconf:etl_date}'
    AND t.vin IS NOT NULL
    AND t.charge_status IN (1, 2)  -- 充电中或充电完成的数据
    AND t.charger_id IS NOT NULL;