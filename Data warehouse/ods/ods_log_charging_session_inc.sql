DROP TABLE IF EXISTS ods_log_charging_session_inc;
CREATE EXTERNAL TABLE ods_log_charging_session_inc (
    `vin` STRING,
    `session_id` STRING COMMENT '充电会话ID',
    `station_id` STRING COMMENT '充电桩ID',
    `start_time` BIGINT,
    `end_time` BIGINT,
    `start_soc` DOUBLE,
    `end_soc` DOUBLE,
    `energy_charged` DOUBLE COMMENT '充入电量(kWh)',
    `stop_reason` INT COMMENT '停止原因:1充满,2拔枪,3故障,4余额不足',
    `max_temp_during` DOUBLE COMMENT '过程最高温度',
    `avg_curr` DOUBLE COMMENT '平均电流'
)
COMMENT '充电行程结算日志表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/warehouse/nev_safety/ods/ods_log_charging_session_inc/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');