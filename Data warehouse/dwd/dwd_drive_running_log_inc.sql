DROP TABLE IF EXISTS dwd_drive_running_log_inc;

CREATE TABLE IF NOT EXISTS dwd_drive_running_log_inc (
    -- 维度字段（含退化维度）
    vin                     STRING          COMMENT 'VIN码（车辆唯一标识）',
    vehicle_sk              BIGINT          COMMENT '车辆代理键（关联dim_vehicle_zip）',
    model_name              STRING          COMMENT '车型名称（退化维度）',
    battery_type            STRING          COMMENT '电池类型（退化维度）',
    province_code           STRING          COMMENT '归属省份代码（退化维度）',
    city_code               STRING          COMMENT '归属城市代码（退化维度）',
    
    -- 时间字段
    collect_time            TIMESTAMP       COMMENT '采集时间（T-BOX上报时间）',
    collect_date            STRING          COMMENT '采集日期 yyyy-MM-dd（冗余，便于查询）',
    collect_hour            INT             COMMENT '采集小时 0-23',
    
    -- 整车状态字段
    vehicle_status          TINYINT         COMMENT '车辆状态：1-启动 2-熄火 3-其他 4-异常',
    charge_status           TINYINT         COMMENT '充电状态：1-停车充电 2-行驶充电 3-未充电 4-充电完成',
    run_mode                TINYINT         COMMENT '运行模式：1-纯电 2-混动 3-燃油',
    speed                   DOUBLE          COMMENT '车速（km/h）',
    odometer                DOUBLE          COMMENT '累计里程（km）',
    
    -- 动力系统字段
    total_voltage           DOUBLE          COMMENT '总电压（V）',
    total_current           DOUBLE          COMMENT '总电流（A）',
    soc                     DOUBLE          COMMENT 'SOC 电池荷电状态（%）',
    dc_status               TINYINT         COMMENT 'DC-DC状态：1-工作 2-断开',
    gear_position           TINYINT         COMMENT '挡位：0-空挡 1-D挡 2-R挡 等',
    insulation_res          INT             COMMENT '绝缘电阻（kΩ）',
    accelerator_pedal       DOUBLE          COMMENT '加速踏板行程（%）',
    brake_pedal_status      TINYINT         COMMENT '制动踏板状态：0-未踩 1-已踩',
    
    -- 电机信息
    motors                  ARRAY<STRUCT<
                                motor_seq: INT,
                                motor_status: TINYINT,
                                controller_temp: DOUBLE,
                                motor_speed: INT,
                                motor_torque: DOUBLE,
                                motor_temp: DOUBLE,
                                input_voltage: DOUBLE,
                                dc_bus_current: DOUBLE
                            >>              COMMENT '电机数组（1-N个电机详细信息）',
    
    -- 主电机关键指标（从motors中提取第一个电机，用于统计）
    main_motor_speed        INT             COMMENT '主电机转速（rpm）',
    main_motor_torque       DOUBLE          COMMENT '主电机转矩（N·m）',
    main_motor_temp         DOUBLE          COMMENT '主电机温度（℃）',
    main_controller_temp    DOUBLE          COMMENT '主控制器温度（℃）',
    
    -- 地理位置字段
    lat                     DOUBLE          COMMENT '纬度',
    lon                     DOUBLE          COMMENT '经度',
    gps_status              TINYINT         COMMENT 'GPS定位状态：0-有效 1-无效',
    is_valid_gps            TINYINT         COMMENT 'GPS清洗标识：1-有效 0-漂移/异常',
    
    -- 数据质量标识
    data_quality_flag       STRING          COMMENT '数据质量标识：NORMAL/GPS_DRIFT/UNIT_ABNORMAL等',
    
    -- ETL元数据
    etl_date                STRING          COMMENT 'ETL处理日期',
    create_time             TIMESTAMP       COMMENT '记录创建时间'
)
COMMENT '车辆行驶实时明细表（事务型事实表）'
PARTITIONED BY (dt STRING COMMENT '分区字段：采集日期 yyyy-MM-dd')
CLUSTERED BY (vin) SORTED BY (vin, collect_time) INTO 32 BUCKETS
STORED AS ORC
TBLPROPERTIES (
    'orc.compress'='SNAPPY',
    'orc.stripe.size'='67108864',
    'orc.create.index'='true'
);

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=1000;
SET hive.exec.max.dynamic.partitions.pernode=500;

INSERT OVERWRITE TABLE dwd_drive_running_log_inc PARTITION(dt)
SELECT
    -- 维度字段
    t.vin,
    COALESCE(v.vehicle_sk, -1) AS vehicle_sk,
    v.model_name,
    v.battery_type,
    v.province_code,
    v.city_code,
    
    -- 时间字段
    t.collect_time,
    FROM_UNIXTIME(UNIX_TIMESTAMP(t.collect_time), 'yyyy-MM-dd') AS collect_date,
    HOUR(t.collect_time) AS collect_hour,
    
    -- 整车状态（保持原值）
    t.vehicle_status,
    t.charge_status,
    t.run_mode,
    t.speed,
    t.odometer / 10.0 AS odometer,  -- 原始单位0.1km，转换为km
    
    -- 动力系统（单位换算）
    t.total_voltage / 10.0 AS total_voltage,  -- 0.1V -> V
    (t.total_current - 10000) / 10.0 AS total_current,  -- 偏移量10000，单位0.1A -> A
    t.soc / 10.0 AS soc,  -- 0.1% -> %
    t.dc_status,
    t.gear_position,
    t.insulation_res,
    t.accelerator_pedal / 10.0 AS accelerator_pedal,  -- 0.1% -> %
    t.brake_pedal_status,
    
    -- 电机数组（保留ARRAY结构）
    t.motors,
    
    -- 主电机指标提取（取第一个电机）
    t.motors[0].motor_speed AS main_motor_speed,
    t.motors[0].motor_torque / 10.0 AS main_motor_torque,
    t.motors[0].motor_temp - 40 AS main_motor_temp,  -- 偏移量40
    t.motors[0].controller_temp - 40 AS main_controller_temp,
    
    -- GPS字段 + 清洗逻辑
    t.lat / 1000000.0 AS lat,  -- 百万分之一度 -> 度
    t.lon / 1000000.0 AS lon,
    t.gps_status,
    CASE 
        WHEN t.lat = 0 OR t.lon = 0 THEN 0
        WHEN ABS(t.lat / 1000000.0) > 90 OR ABS(t.lon / 1000000.0) > 180 THEN 0
        WHEN t.gps_status = 1 THEN 0  -- GPS无效
        ELSE 1
    END AS is_valid_gps,
    
    -- 数据质量标识
    CASE
        WHEN t.lat = 0 OR t.lon = 0 OR ABS(t.lat / 1000000.0) > 90 THEN 'GPS_DRIFT'
        WHEN t.total_voltage IS NULL OR t.soc IS NULL THEN 'CRITICAL_FIELD_NULL'
        ELSE 'NORMAL'
    END AS data_quality_flag,
    
    -- ETL元数据
    '${hiveconf:etl_date}' AS etl_date,
    CURRENT_TIMESTAMP() AS create_time,
    
    -- 动态分区字段
    FROM_UNIXTIME(UNIX_TIMESTAMP(t.collect_time), 'yyyy-MM-dd') AS dt
    
FROM 
    ods_log_vehicle_track_inc t
LEFT JOIN 
    dim_vehicle_zip v 
    ON t.vin = v.vin 
    AND FROM_UNIXTIME(UNIX_TIMESTAMP(t.collect_time), 'yyyy-MM-dd') BETWEEN v.start_date AND v.end_date
WHERE 
    t.dt = '${hiveconf:etl_date}'  -- 处理指定日期的ODS数据
    AND t.collect_time IS NOT NULL
    AND t.vin IS NOT NULL
    AND LENGTH(t.vin) = 17;