DROP TABLE IF EXISTS ods_biz_sim_card_full;
CREATE EXTERNAL TABLE ods_biz_sim_card_full (
    `iccid` STRING COMMENT 'ICCID',
    `vin` STRING COMMENT '绑定车辆VIN',
    `operator` STRING COMMENT '运营商: CMCC/CUCC/CTCC',
    `active_date` STRING COMMENT '激活日期',
    `flow_limit` INT COMMENT '月流量限制(MB)',
    `status` INT COMMENT '状态: 1正常, 2停机'
)
COMMENT 'SIM卡信息全量表'
PARTITIONED BY (`dt` STRING)
STORED AS ORC
TBLPROPERTIES ("orc.compress"="SNAPPY");