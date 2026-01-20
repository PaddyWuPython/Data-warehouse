DROP TABLE IF EXISTS ods_biz_alarm_rule_config_full;
CREATE EXTERNAL TABLE ods_biz_alarm_rule_config_full (
    `rule_id` STRING COMMENT '规则ID',
    `rule_name` STRING COMMENT '规则名称',
    `metric_name` STRING COMMENT '监控指标',
    `threshold_value` DOUBLE COMMENT '阈值',
    `severity_level` INT COMMENT '报警等级',
    `enable_status` INT COMMENT '启用状态: 0停用, 1启用',
    `version` STRING COMMENT '版本号'
)
COMMENT '报警规则配置全量表'
PARTITIONED BY (`dt` STRING)
STORED AS ORC
TBLPROPERTIES ("orc.compress"="SNAPPY");