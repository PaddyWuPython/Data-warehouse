DROP TABLE IF EXISTS ods_biz_charging_station_full;
CREATE EXTERNAL TABLE ods_biz_charging_station_full (
    `station_id` STRING COMMENT '充电站ID',
    `station_name` STRING COMMENT '充电站名称',
    `operator_id` STRING COMMENT '运营商ID',
    `lat` DOUBLE COMMENT '纬度',
    `lon` DOUBLE COMMENT '经度',
    `pile_count` INT COMMENT '充电桩数量',
    `region_code` STRING COMMENT '所在地区',
    `open_time` STRING COMMENT '开放时间'
)
COMMENT '充电站主数据全量表'
PARTITIONED BY (`dt` STRING)
STORED AS ORC
TBLPROPERTIES ("orc.compress"="SNAPPY");