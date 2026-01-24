DROP TABLE IF EXISTS dwd_safe_alarm_detail_inc;

CREATE TABLE IF NOT EXISTS dwd_safe_alarm_detail_inc (
    -- 主键
    alarm_record_id             STRING      COMMENT '报警记录ID（vin+alarm_time+fault_code MD5）',
    
    -- 维度字段
    vin                         STRING      COMMENT 'VIN码',
    vehicle_sk                  BIGINT      COMMENT '车辆代理键',
    model_name                  STRING      COMMENT '车型名称',
    
    -- 时间字段
    alarm_time                  TIMESTAMP   COMMENT '报警时间',
    alarm_date                  STRING      COMMENT '报警日期',
    alarm_hour                  INT         COMMENT '报警小时',
    
    -- 故障信息
    fault_hex_code              STRING      COMMENT '故障码（Hex格式）',
    fault_level                 TINYINT     COMMENT '故障等级：1-一般 2-较重 3-严重',
    alarm_level                 TINYINT     COMMENT '报警级别',
    
    -- 故障维度退化（从故障码字典表关联）
    fault_name                  STRING      COMMENT '故障名称',
    fault_category              STRING      COMMENT '故障类别：电池/电机/整车/其他',
    solution_guide              STRING      COMMENT '维修建议',
    is_safety_related           TINYINT     COMMENT '是否安全相关：1-是 0-否',
    
    -- 车辆状态快照（报警时刻的车辆状态）
    speed                       DOUBLE      COMMENT '车速（km/h）',
    soc                         DOUBLE      COMMENT 'SOC（%）',
    total_voltage               DOUBLE      COMMENT '总电压（V）',
    total_current               DOUBLE      COMMENT '总电流（A）',
    max_temp                    DOUBLE      COMMENT '最高温度（℃）',
    
    -- 地理位置
    lat                         DOUBLE      COMMENT '纬度',
    lon                         DOUBLE      COMMENT '经度',
    province                    STRING      COMMENT '省份',
    city                        STRING      COMMENT '城市',
    
    -- ETL元数据
    etl_date                    STRING      COMMENT 'ETL处理日期',
    create_time                 TIMESTAMP   COMMENT '记录创建时间'
)
COMMENT '车辆故障报警明细表（事务型事实表）'
PARTITIONED BY (dt STRING COMMENT '分区字段：报警日期 yyyy-MM-dd')
STORED AS ORC
TBLPROPERTIES (
    'orc.compress'='SNAPPY'
);

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE dwd_safe_alarm_detail_inc PARTITION(dt)
SELECT
    -- 主键
    MD5(CONCAT(t.vin, t.collect_time, fault_code)) AS alarm_record_id,
    
    -- 维度
    t.vin,
    COALESCE(v.vehicle_sk, -1) AS vehicle_sk,
    v.model_name,
    
    -- 时间
    t.collect_time AS alarm_time,
    FROM_UNIXTIME(UNIX_TIMESTAMP(t.collect_time), 'yyyy-MM-dd') AS alarm_date,
    HOUR(t.collect_time) AS alarm_hour,
    
    -- 故障信息
    fault_code AS fault_hex_code,
    t.alarm_level AS fault_level,
    t.alarm_level,
    
    -- 故障维度退化
    d.fault_name,
    d.fault_category,
    d.solution_guide,
    d.is_safety_related,
    
    -- 车辆状态快照
    t.speed,
    t.soc / 10.0 AS soc,
    t.total_voltage / 10.0 AS total_voltage,
    (t.total_current - 10000) / 10.0 AS total_current,
    array_max(TRANSFORM(t.probe_temp_list, x -> x - 40.0)) AS max_temp,
    
    -- 位置
    t.lat / 1000000.0 AS lat,
    t.lon / 1000000.0 AS lon,
    NULL AS province,
    NULL AS city,
    
    -- ETL元数据
    '${hiveconf:etl_date}' AS etl_date,
    CURRENT_TIMESTAMP() AS create_time,
    
    -- 动态分区
    FROM_UNIXTIME(UNIX_TIMESTAMP(t.collect_time), 'yyyy-MM-dd') AS dt
    
FROM 
    ods_log_vehicle_track_inc t
LATERAL VIEW EXPLODE(t.fault_codes) fault_table AS fault_code  -- 炸裂故障码数组
LEFT JOIN 
    dim_vehicle_zip v 
    ON t.vin = v.vin 
    AND FROM_UNIXTIME(UNIX_TIMESTAMP(t.collect_time), 'yyyy-MM-dd') BETWEEN v.start_date AND v.end_date
LEFT JOIN
    ods_biz_fault_code_dict_full d
    ON fault_code = d.dtc_code
    AND d.dt = '${hiveconf:etl_date}'
WHERE 
    t.dt = '${hiveconf:etl_date}'
    AND t.vin IS NOT NULL
    AND t.fault_codes IS NOT NULL
    AND SIZE(t.fault_codes) > 0;  -- 只保留有故障码的记录