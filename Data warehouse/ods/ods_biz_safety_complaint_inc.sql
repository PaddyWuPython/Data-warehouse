DROP TABLE IF EXISTS ods_biz_safety_complaint_inc;
CREATE EXTERNAL TABLE ods_biz_safety_complaint_inc (
    `database` STRING COMMENT '原数据库',
    `table` STRING COMMENT '原表名',
    `type` STRING COMMENT '操作类型: insert/update/delete',
    `ts` BIGINT COMMENT 'Binlog时间戳',
    `data` STRUCT<
        complaint_id:STRING, 
        user_id:STRING, 
        vin:STRING, 
        complaint_time:STRING, 
        complaint_type:STRING, 
        description:STRING, 
        status:INT,
        close_time:STRING
    > COMMENT '业务数据'
)
COMMENT '用户安全投诉工单增量表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE;