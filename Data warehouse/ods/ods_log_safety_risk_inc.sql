DROP TABLE IF EXISTS ods_log_safety_risk_inc;
CREATE EXTERNAL TABLE ods_log_safety_risk_inc (
    `vin` STRING,
    `event_time` BIGINT,
    `risk_source` STRING COMMENT '来源: VEHICLE(车辆感应), COCKPIT(座舱视觉)',
    `risk_type` STRING COMMENT '类型: HARD_BRAKE, RAPID_ACCEL, FATIGUE, DISTRACTION, NO_SEATBELT',
    `metrics` STRUCT<
        start_speed: DOUBLE,
        end_speed: DOUBLE,
        g_value: DOUBLE COMMENT '加速度值',
        duration_ms: INT COMMENT '持续时间',
        confidence: DOUBLE COMMENT '置信度'
    > COMMENT '风险度量指标',
    `location` STRUCT<
        lat: DOUBLE,
        lon: DOUBLE,
        road_type: STRING COMMENT '道路类型'
    >
)
COMMENT '驾驶与座舱安全风险事件表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/warehouse/nev_safety/ods/ods_log_safety_risk_inc/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');