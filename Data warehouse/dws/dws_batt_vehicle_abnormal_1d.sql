CREATE TABLE IF NOT EXISTS dws_batt_vehicle_abnormal_1d (
    vin                         STRING      COMMENT 'VIN码',
    stat_date                   STRING      COMMENT '统计日期',
    voltage_abnormal_cnt        INT         COMMENT '电压异常次数',
    temp_abnormal_cnt           INT         COMMENT '温度异常次数',
    thermal_risk_cnt            INT         COMMENT '热失控风险次数',
    consistency_risk_cnt        INT         COMMENT '一致性风险次数',
    insulation_fault_cnt        INT         COMMENT '绝缘故障次数',
    total_abnormal_duration_min INT         COMMENT '总异常持续时长（分钟）',
    max_continuous_abnormal_min INT         COMMENT '最长连续异常时长',
    max_voltage_diff_value      DOUBLE      COMMENT '异常时最大压差',
    max_temp_value              DOUBLE      COMMENT '异常时最高温度',
    is_alarm_triggered          TINYINT     COMMENT '是否触发电池报警'
)
COMMENT '电池异常事件统计日表（DWS层）'
PARTITIONED BY (dt STRING)
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY');

