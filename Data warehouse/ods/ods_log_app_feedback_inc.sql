DROP TABLE IF EXISTS ods_log_app_feedback_inc;
CREATE EXTERNAL TABLE ods_log_app_feedback_inc (
    `user_id` STRING,
    `vin` STRING,
    `submit_time` BIGINT,
    `app_ver` STRING,
    `phone_model` STRING,
    `feedback_type` STRING COMMENT '类型: FAULT, SUGGESTION, COMPLAINT',
    `content` STRING COMMENT '反馈内容',
    `media_urls` ARRAY<STRING> COMMENT '上传的图片/视频链接列表',
    `geo_info` STRUCT<lat:DOUBLE, lon:DOUBLE, city:STRING>
)
COMMENT '用户APP反馈日志表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/warehouse/nev_safety/ods/ods_log_app_feedback_inc/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');