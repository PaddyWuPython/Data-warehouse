DROP TABLE IF EXISTS dwd_drive_behavior_event_inc;

CREATE TABLE IF NOT EXISTS dwd_drive_behavior_event_inc (
    -- 主键与标识
    event_id                STRING          COMMENT '事件唯一ID（vin+event_time+event_type MD5）',
    vin                     STRING          COMMENT 'VIN码',
    vehicle_sk              BIGINT          COMMENT '车辆代理键',
    user_id                 STRING          COMMENT '驾驶员ID（如有）',
    
    -- 退化维度
    model_name              STRING          COMMENT '车型名称',
    province_code           STRING          COMMENT '省份代码',
    city_code               STRING          COMMENT '城市代码',
    
    -- 时间字段
    event_time              TIMESTAMP       COMMENT '事件发生时间',
    event_date              STRING          COMMENT '事件日期 yyyy-MM-dd',
    event_hour              INT             COMMENT '事件小时',
    
    -- 事件属性
    event_type              STRING          COMMENT '事件类型：HARD_BRAKE/HARD_ACCEL/SHARP_TURN/SPEEDING/FATIGUE/UNBUCKLED',
    risk_level              STRING          COMMENT '风险等级：HIGH/MEDIUM/LOW',
    
    -- 事件度量
    duration_sec            INT             COMMENT '事件持续时长（秒）',
    start_speed             DOUBLE          COMMENT '事件起始车速（km/h）',
    end_speed               DOUBLE          COMMENT '事件结束车速（km/h）',
    max_speed               DOUBLE          COMMENT '事件期间最大车速（km/h）',
    acceleration_g          DOUBLE          COMMENT '加速度G值（急刹/急加速）',
    turn_angle              DOUBLE          COMMENT '转向角度（急转弯）',
    g_value                 DOUBLE          COMMENT '瞬时G值',
    
    -- 地理位置
    lat                     DOUBLE          COMMENT '纬度',
    lon                     DOUBLE          COMMENT '经度',
    province                STRING          COMMENT '省份名称',
    city                    STRING          COMMENT '城市名称',
    
    -- 环境信息
    weather                 STRING          COMMENT '天气情况',
    road_type               STRING          COMMENT '道路类型：高速/城市/乡村',
    
    -- ETL元数据
    etl_date                STRING          COMMENT 'ETL处理日期',
    create_time             TIMESTAMP       COMMENT '记录创建时间'
)
COMMENT '驾驶行为风险事件表（事务型事实表）'
PARTITIONED BY (dt STRING COMMENT '分区字段：事件日期 yyyy-MM-dd')
STORED AS ORC
TBLPROPERTIES (
    'orc.compress'='SNAPPY'
);

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE dwd_drive_behavior_event_inc PARTITION(dt)
SELECT
    -- 生成事件ID（保证唯一性）
    MD5(CONCAT(t.vin, t.event_time, t.risk_type)) AS event_id,
    t.vin,
    COALESCE(v.vehicle_sk, -1) AS vehicle_sk,
    t.user_id,
    
    -- 退化维度
    v.model_name,
    v.province_code,
    v.city_code,
    
    -- 时间字段
    t.event_time,
    FROM_UNIXTIME(UNIX_TIMESTAMP(t.event_time), 'yyyy-MM-dd') AS event_date,
    HOUR(t.event_time) AS event_hour,
    
    -- 事件属性映射
    CASE t.risk_type
        WHEN 'HARD_BRAKE' THEN '急刹车'
        WHEN 'HARD_ACCEL' THEN '急加速'
        WHEN 'SHARP_TURN' THEN '急转弯'
        WHEN 'SPEEDING' THEN '超速'
        WHEN 'FATIGUE' THEN '疲劳驾驶'
        WHEN 'UNBUCKLED' THEN '未系安全带'
        ELSE t.risk_type
    END AS event_type,
    
    -- 风险等级判断
    CASE 
        WHEN t.g_value >= 0.6 THEN 'HIGH'
        WHEN t.g_value >= 0.4 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS risk_level,
    
    -- 度量字段
    t.duration,
    t.start_speed,
    t.end_speed,
    t.max_speed,
    ABS(t.end_speed - t.start_speed) / NULLIF(t.duration, 0) * 3.6 AS acceleration_g,  -- 加速度换算
    t.turn_angle,
    t.g_value,
    
    -- 地理位置
    t.lat / 1000000.0 AS lat,
    t.lon / 1000000.0 AS lon,
    t.province,
    t.city,
    
    -- 环境信息
    t.weather,
    t.road_type,
    
    -- ETL元数据
    '${hiveconf:etl_date}' AS etl_date,
    CURRENT_TIMESTAMP() AS create_time,
    
    -- 动态分区
    FROM_UNIXTIME(UNIX_TIMESTAMP(t.event_time), 'yyyy-MM-dd') AS dt
    
FROM 
    ods_log_safety_risk_inc t
LEFT JOIN 
    dim_vehicle_zip v 
    ON t.vin = v.vin 
    AND FROM_UNIXTIME(UNIX_TIMESTAMP(t.event_time), 'yyyy-MM-dd') BETWEEN v.start_date AND v.end_date
WHERE 
    t.dt = '${hiveconf:etl_date}'
    AND t.vin IS NOT NULL
    AND t.event_time IS NOT NULL;