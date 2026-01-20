DROP TABLE IF EXISTS ods_biz_charging_pile_full;
CREATE EXTERNAL TABLE ods_biz_charging_pile_full (
    `pile_id` STRING COMMENT '充电桩ID',
    `station_id` STRING COMMENT '所属充电站ID',
    `power_rating` DOUBLE COMMENT '额定功率(kW)',
    `connector_type` INT COMMENT '接口类型: 1国标直流, 2国标交流',
    `voltage_platform` INT COMMENT '电压平台: 400V/800V',
    `status` INT COMMENT '状态: 0离线, 1空闲, 2充电中, 3故障'
)
COMMENT '充电桩主数据全量表'
PARTITIONED BY (`dt` STRING)
STORED AS ORC
TBLPROPERTIES ("orc.compress"="SNAPPY");