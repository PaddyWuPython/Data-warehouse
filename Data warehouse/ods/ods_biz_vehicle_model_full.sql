DROP TABLE IF EXISTS ods_biz_vehicle_model_full;
CREATE EXTERNAL TABLE ods_biz_vehicle_model_full (
    `model_id` STRING COMMENT '车型ID',
    `model_name` STRING COMMENT '车型名称',
    `series_name` STRING COMMENT '车系名称',
    `battery_type` STRING COMMENT '电池类型: 三元锂/磷酸铁锂',
    `battery_capacity` DOUBLE COMMENT '电池额定容量(kWh)',
    `motor_type` STRING COMMENT '电机类型: 永磁同步/交流异步',
    `nedc_range` INT COMMENT 'NEDC续航里程(km)'
)
COMMENT '车型配置全量表'
PARTITIONED BY (`dt` STRING)
STORED AS ORC
TBLPROPERTIES ("orc.compress"="SNAPPY");