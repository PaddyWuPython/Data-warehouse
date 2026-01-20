DROP TABLE IF EXISTS ods_log_vehicle_sys_inc;
CREATE EXTERNAL TABLE ods_log_vehicle_sys_inc (
    `common` STRUCT<
        vin: STRING,
        ts: BIGINT,
        msg_id: STRING
    >,
    `event_type` STRING COMMENT '事件类型: LOGIN, LOGOUT, ERROR',
    `login` STRUCT<
        iccid: STRING,
        proto_ver: STRING COMMENT '协议版本',
        sys_code: STRING COMMENT '系统编码',
        soft_ver: STRING COMMENT '软件版本'
    > COMMENT '登入信息',
    `logout` STRUCT<
        serial_no: INT COMMENT '登出流水号',
        duration: INT COMMENT '本次连接时长(秒)'
    > COMMENT '登出信息',
    `error` STRUCT<
        err_code: INT,
        err_msg: STRING,
        raw_log: STRING COMMENT '原始错误日志'
    > COMMENT '终端异常信息'
)
COMMENT '车辆系统事件流水表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/warehouse/nev_safety/ods/ods_log_vehicle_sys_inc/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');