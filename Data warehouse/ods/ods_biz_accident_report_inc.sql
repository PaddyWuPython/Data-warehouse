DROP TABLE IF EXISTS ods_biz_accident_report_inc;
CREATE EXTERNAL TABLE ods_biz_accident_report_inc (
    `database` STRING,
    `table` STRING,
    `type` STRING,
    `ts` BIGINT,
    `data` STRUCT<
        report_id:STRING,
        vin:STRING,
        accident_time:STRING,
        location:STRING,
        accident_level:STRING,
        road_condition:STRING,
        weather:STRING,
        description:STRING,
        is_fire:INT,
        casualties:INT
    >
)
COMMENT '事故上报记录增量表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE;