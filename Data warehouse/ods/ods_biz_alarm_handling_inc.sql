DROP TABLE IF EXISTS ods_biz_alarm_handling_inc;
CREATE EXTERNAL TABLE ods_biz_alarm_handling_inc (
    `database` STRING,
    `table` STRING,
    `type` STRING,
    `ts` BIGINT,
    `data` STRUCT<
        handle_id:STRING,
        vin:STRING,
        alarm_time:STRING,
        response_start_time:STRING,
        response_end_time:STRING,
        handle_result:STRING,
        operator_id:STRING,
        remark:STRING
    >
)
COMMENT '报警处置记录增量表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE;