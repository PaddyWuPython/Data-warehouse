DROP TABLE IF EXISTS ods_biz_maintenance_order_inc;
CREATE EXTERNAL TABLE ods_biz_maintenance_order_inc (
    `database` STRING,
    `table` STRING,
    `type` STRING,
    `ts` BIGINT,
    `data` STRUCT<
        order_id:STRING,
        vin:STRING,
        repair_start_time:STRING,
        repair_end_time:STRING,
        mileage:DOUBLE,
        repair_parts_list:STRING,
        cost:DOUBLE,
        dealer_id:STRING
    >
)
COMMENT '维修保养工单增量表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE;