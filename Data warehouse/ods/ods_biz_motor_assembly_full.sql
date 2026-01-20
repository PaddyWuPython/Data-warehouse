DROP TABLE IF EXISTS ods_biz_motor_assembly_full;
CREATE EXTERNAL TABLE ods_biz_motor_assembly_full (
    `motor_code` STRING COMMENT '电机编码',
    `vin` STRING COMMENT '关联车辆VIN',
    `supplier_id` STRING COMMENT '供应商ID',
    `position` INT COMMENT '位置: 1前驱, 2后驱',
    `peak_power` DOUBLE COMMENT '峰值功率(kW)',
    `max_torque` DOUBLE COMMENT '最大扭矩(N·m)'
)
COMMENT '电机总成全量表'
PARTITIONED BY (`dt` STRING)
STORED AS ORC
TBLPROPERTIES ("orc.compress"="SNAPPY");