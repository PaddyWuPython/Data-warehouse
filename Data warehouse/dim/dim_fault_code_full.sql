DROP TABLE IF EXISTS dim_fault_code_full;
CREATE EXTERNAL TABLE dim_fault_code_full (
    `fault_sk` STRING COMMENT '故障代理键',
    `dtc_code` STRING COMMENT '故障码Hex',
    `fault_name` STRING COMMENT '故障中文名称',
    `risk_level` INT COMMENT '风险等级(1-3)',
    `risk_level_desc` STRING COMMENT '风险等级描述',
    `system_source` STRING COMMENT '所属系统(BMS/VCU/MCU)',
    `solution_guide` STRING COMMENT '维修建议'
)
COMMENT '故障码字典维度表'
STORED AS ORC;

-- 数据装载
INSERT OVERWRITE TABLE dim_fault_code_full
SELECT 
    dtc_code as fault_sk, -- Hex码唯一
    dtc_code,
    description as fault_name,
    risk_level,
    CASE risk_level 
        WHEN 1 THEN '一般故障(L1)' 
        WHEN 2 THEN '严重故障(L2)' 
        WHEN 3 THEN '致命故障(L3)' 
        ELSE '未知' 
    END as risk_level_desc,
    system_module as system_source,
    solution_guide
FROM ods_biz_fault_code_dict_full
WHERE dt='${do_date}'; -- 取最新分区