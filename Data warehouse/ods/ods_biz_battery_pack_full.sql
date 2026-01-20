DROP TABLE IF EXISTS ods_biz_battery_pack_full;
CREATE EXTERNAL TABLE ods_biz_battery_pack_full (
    `pack_code` STRING COMMENT '电池包编码',
    `vin` STRING COMMENT '关联车辆VIN',
    `supplier_id` STRING COMMENT '供应商ID',
    `cell_model` STRING COMMENT '电芯型号',
    `cell_count` INT COMMENT '电芯数量',
    `group_count` INT COMMENT '模组数量',
    `production_batch` STRING COMMENT '生产批次号',
    `rated_voltage` DOUBLE COMMENT '额定电压(V)'
)
COMMENT '电池包BOM全量表'
PARTITIONED BY (`dt` STRING)
STORED AS ORC
TBLPROPERTIES ("orc.compress"="SNAPPY");