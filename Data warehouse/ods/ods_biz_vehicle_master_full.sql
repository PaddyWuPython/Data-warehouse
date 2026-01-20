DROP TABLE IF EXISTS ods_biz_vehicle_master_full;
CREATE EXTERNAL TABLE ods_biz_vehicle_master_full (
    `vin` STRING COMMENT '车辆唯一识别代码',
    `plate_no` STRING COMMENT '车牌号',
    `production_date` STRING COMMENT '生产日期',
    `sales_date` STRING COMMENT '销售日期',
    `color` STRING COMMENT '车身颜色',
    `region_code` STRING COMMENT '注册地行政区划码',
    `create_time` STRING COMMENT '创建时间',
    `update_time` STRING COMMENT '更新时间'
)
COMMENT '车辆主档案全量表'
PARTITIONED BY (`dt` STRING)
STORED AS ORC
TBLPROPERTIES ("orc.compress"="SNAPPY");