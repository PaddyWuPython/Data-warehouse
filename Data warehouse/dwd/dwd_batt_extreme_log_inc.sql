DROP TABLE IF EXISTS dwd_batt_extreme_log_inc;

CREATE TABLE IF NOT EXISTS dwd_batt_extreme_log_inc (
    -- 维度字段
    vin                         STRING      COMMENT 'VIN码',
    vehicle_sk                  BIGINT      COMMENT '车辆代理键',
    
    -- 时间字段
    collect_time                TIMESTAMP   COMMENT '采集时间',
    collect_date                STRING      COMMENT '采集日期',
    
    -- 最高电压信息
    max_voltage_system_no       INT         COMMENT '最高电压电池子系统号',
    max_voltage_cell_no         INT         COMMENT '最高电压电池单体代号',
    max_voltage_value           DOUBLE      COMMENT '最高电压值（V）',
    
    -- 最低电压信息
    min_voltage_system_no       INT         COMMENT '最低电压电池子系统号',
    min_voltage_cell_no         INT         COMMENT '最低电压电池单体代号',
    min_voltage_value           DOUBLE      COMMENT '最低电压值（V）',
    
    -- 电压压差
    voltage_diff                DOUBLE      COMMENT '电压压差（V）',
    
    -- 最高温度信息
    max_temp_system_no          INT         COMMENT '最高温度子系统号',
    max_temp_probe_no           INT         COMMENT '最高温度探针序号',
    max_temp_value              DOUBLE      COMMENT '最高温度值（℃）',
    
    -- 最低温度信息
    min_temp_system_no          INT         COMMENT '最低温度子系统号',
    min_temp_probe_no           INT         COMMENT '最低温度探针序号',
    min_temp_value              DOUBLE      COMMENT '最低温度值（℃）',
    
    -- 温度差
    temp_diff                   DOUBLE      COMMENT '温度差（℃）',
    
    -- 整包状态
    pack_voltage                DOUBLE      COMMENT '电池包总电压（V）',
    pack_current                DOUBLE      COMMENT '电池包总电流（A）',
    soc                         DOUBLE      COMMENT 'SOC（%）',
    
    -- 风险标识
    is_thermal_risk             TINYINT     COMMENT '热失控风险：1-是 0-否（温度>60℃或温差>15℃）',
    is_consistency_risk         TINYINT     COMMENT '一致性风险：1-是 0-否（压差>150mV）',
    
    -- ETL元数据
    etl_date                    STRING      COMMENT 'ETL处理日期',
    create_time                 TIMESTAMP   COMMENT '记录创建时间'
)
COMMENT '电池极值与异常表（事务型事实表）'
PARTITIONED BY (dt STRING COMMENT '分区字段：采集日期 yyyy-MM-dd')
STORED AS ORC
TBLPROPERTIES (
    'orc.compress'='SNAPPY'
);

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE dwd_batt_extreme_log_inc PARTITION(dt)
SELECT
    -- 维度字段
    t.vin,
    COALESCE(v.vehicle_sk, -1) AS vehicle_sk,
    
    -- 时间字段
    t.collect_time,
    FROM_UNIXTIME(UNIX_TIMESTAMP(t.collect_time), 'yyyy-MM-dd') AS collect_date,
    
    -- 最高电压（假设ODS中有BMS计算的极值字段）
    t.max_voltage_system_no,
    t.max_voltage_cell_no,
    t.max_voltage_value / 1000.0 AS max_voltage_value,  -- mV -> V
    
    -- 最低电压
    t.min_voltage_system_no,
    t.min_voltage_cell_no,
    t.min_voltage_value / 1000.0 AS min_voltage_value,
    
    -- 压差
    (t.max_voltage_value - t.min_voltage_value) / 1000.0 AS voltage_diff,
    
    -- 最高温度
    t.max_temp_system_no,
    t.max_temp_probe_no,
    t.max_temp_value - 40 AS max_temp_value,  -- 偏移量40
    
    -- 最低温度
    t.min_temp_system_no,
    t.min_temp_probe_no,
    t.min_temp_value - 40 AS min_temp_value,
    
    -- 温差
    (t.max_temp_value - t.min_temp_value) AS temp_diff,
    
    -- 整包状态
    t.pack_voltage / 10.0 AS pack_voltage,
    (t.pack_current - 10000) / 10.0 AS pack_current,
    t.soc / 10.0 AS soc,
    
    -- 风险标识
    CASE 
        WHEN (t.max_temp_value - 40) > 60 OR (t.max_temp_value - t.min_temp_value) > 15 THEN 1
        ELSE 0
    END AS is_thermal_risk,
    
    CASE 
        WHEN (t.max_voltage_value - t.min_voltage_value) > 150 THEN 1  -- 压差>150mV
        ELSE 0
    END AS is_consistency_risk,
    
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
WHERE 
    t.dt = '${hiveconf:etl_date}'
    AND t.vin IS NOT NULL
    AND t.max_voltage_value IS NOT NULL;