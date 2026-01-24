DROP TABLE IF EXISTS dwd_batt_cell_log_inc;

CREATE TABLE IF NOT EXISTS dwd_batt_cell_log_inc (
    -- 维度字段
    vin                     STRING          COMMENT 'VIN码',
    vehicle_sk              BIGINT          COMMENT '车辆代理键',
    battery_pack_code       STRING          COMMENT '电池包编码（退化维度）',
    battery_type            STRING          COMMENT '电池类型（退化维度）',
    cell_count              INT             COMMENT '单体电芯总数（退化维度）',
    
    -- 时间字段
    collect_time            TIMESTAMP       COMMENT '采集时间',
    collect_date            STRING          COMMENT '采集日期',
    
    -- 单体电压数组（保留ARRAY，不炸裂）
    cell_voltages           ARRAY<DOUBLE>   COMMENT '单体电压数组（V）',
    
    -- 单体温度数组
    cell_temps              ARRAY<DOUBLE>   COMMENT '探针温度数组（℃）',
    
    -- 派生指标（从数组计算得出，便于快速查询）
    max_cell_voltage        DOUBLE          COMMENT '最高单体电压（V）',
    min_cell_voltage        DOUBLE          COMMENT '最低单体电压（V）',
    voltage_diff            DOUBLE          COMMENT '压差（V）：max - min，一致性核心指标',
    avg_cell_voltage        DOUBLE          COMMENT '平均单体电压（V）',
    
    max_cell_temp           DOUBLE          COMMENT '最高温度（℃）',
    min_cell_temp           DOUBLE          COMMENT '最低温度（℃）',
    temp_diff               DOUBLE          COMMENT '温差（℃）',
    avg_temp                DOUBLE          COMMENT '平均温度（℃）',
    
    -- 整包级别信息
    pack_voltage            DOUBLE          COMMENT '电池包总电压（V）',
    pack_current            DOUBLE          COMMENT '电池包总电流（A）',
    soc                     DOUBLE          COMMENT 'SOC（%）',
    soh                     DOUBLE          COMMENT 'SOH 健康度（%）',
    
    -- 异常标识
    is_voltage_abnormal     TINYINT         COMMENT '电压异常标识：1-压差超阈值 0-正常',
    is_temp_abnormal        TINYINT         COMMENT '温度异常标识：1-温差或绝对值超阈值 0-正常',
    
    -- ETL元数据
    etl_date                STRING          COMMENT 'ETL处理日期',
    create_time             TIMESTAMP       COMMENT '记录创建时间'
)
COMMENT '电池单体监测明细表（事务型事实表）'
PARTITIONED BY (dt STRING COMMENT '分区字段：采集日期 yyyy-MM-dd')
CLUSTERED BY (vin) SORTED BY (vin, collect_time) INTO 16 BUCKETS
STORED AS ORC
TBLPROPERTIES (
    'orc.compress'='SNAPPY'
);

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE dwd_batt_cell_log_inc PARTITION(dt)
SELECT
    -- 维度字段
    t.vin,
    COALESCE(v.vehicle_sk, -1) AS vehicle_sk,
    b.pack_code AS battery_pack_code,
    b.battery_type,
    b.cell_count,
    
    -- 时间字段
    t.collect_time,
    FROM_UNIXTIME(UNIX_TIMESTAMP(t.collect_time), 'yyyy-MM-dd') AS collect_date,
    
    -- 单体电压数组（从ODS的cell_volt_list转换单位）
    TRANSFORM(t.cell_volt_list, x -> x / 1000.0) AS cell_voltages,  -- mV -> V
    
    -- 单体温度数组（从ODS的probe_temp_list转换，去除偏移量）
    TRANSFORM(t.probe_temp_list, x -> x - 40.0) AS cell_temps,  -- 偏移量40，单位℃
    
    -- 派生指标：从数组计算极值
    array_max(TRANSFORM(t.cell_volt_list, x -> x / 1000.0)) AS max_cell_voltage,
    array_min(TRANSFORM(t.cell_volt_list, x -> x / 1000.0)) AS min_cell_voltage,
    array_max(TRANSFORM(t.cell_volt_list, x -> x / 1000.0)) - 
        array_min(TRANSFORM(t.cell_volt_list, x -> x / 1000.0)) AS voltage_diff,
    (aggregate(TRANSFORM(t.cell_volt_list, x -> x / 1000.0), 
        CAST(0.0 AS DOUBLE), (acc, x) -> acc + x) / SIZE(t.cell_volt_list)) AS avg_cell_voltage,
    
    array_max(TRANSFORM(t.probe_temp_list, x -> x - 40.0)) AS max_cell_temp,
    array_min(TRANSFORM(t.probe_temp_list, x -> x - 40.0)) AS min_cell_temp,
    array_max(TRANSFORM(t.probe_temp_list, x -> x - 40.0)) - 
        array_min(TRANSFORM(t.probe_temp_list, x -> x - 40.0)) AS temp_diff,
    (aggregate(TRANSFORM(t.probe_temp_list, x -> x - 40.0), 
        CAST(0.0 AS DOUBLE), (acc, x) -> acc + x) / SIZE(t.probe_temp_list)) AS avg_temp,
    
    -- 整包信息
    t.pack_voltage / 10.0 AS pack_voltage,
    (t.pack_current - 10000) / 10.0 AS pack_current,
    t.soc / 10.0 AS soc,
    t.soh / 10.0 AS soh,
    
    -- 异常标识（基于业务规则）
    CASE 
        WHEN (array_max(TRANSFORM(t.cell_volt_list, x -> x / 1000.0)) - 
              array_min(TRANSFORM(t.cell_volt_list, x -> x / 1000.0))) > 0.1 THEN 1  -- 压差>100mV
        ELSE 0
    END AS is_voltage_abnormal,
    
    CASE 
        WHEN array_max(TRANSFORM(t.probe_temp_list, x -> x - 40.0)) > 55 THEN 1  -- 最高温>55℃
        WHEN (array_max(TRANSFORM(t.probe_temp_list, x -> x - 40.0)) - 
              array_min(TRANSFORM(t.probe_temp_list, x -> x - 40.0))) > 10 THEN 1  -- 温差>10℃
        ELSE 0
    END AS is_temp_abnormal,
    
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
    ods_biz_battery_pack_full b
    ON v.battery_pack_code = b.pack_code
    AND b.dt = '${hiveconf:etl_date}'
WHERE 
    t.dt = '${hiveconf:etl_date}'
    AND t.vin IS NOT NULL
    AND t.cell_volt_list IS NOT NULL
    AND SIZE(t.cell_volt_list) > 0;