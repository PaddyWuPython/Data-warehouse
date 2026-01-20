DROP TABLE IF EXISTS ods_biz_fault_code_dict_full;
CREATE EXTERNAL TABLE ods_biz_fault_code_dict_full (
    `dtc_code` STRING COMMENT '故障码(Hex)',
    `description` STRING COMMENT '中文描述',
    `solution_guide` STRING COMMENT '维修指导',
    `system_module` STRING COMMENT '所属系统: BMS/MCU/VCU',
    `risk_level` INT COMMENT '风险等级'
)
COMMENT '故障码字典全量表'
PARTITIONED BY (`dt` STRING)
STORED AS ORC
TBLPROPERTIES ("orc.compress"="SNAPPY");