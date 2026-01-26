CREATE TABLE IF NOT EXISTS dws_safe_vehicle_alarm_1d (
    vin                         STRING      COMMENT 'VIN码',
    vehicle_sk                  BIGINT      COMMENT '车辆代理键',
    model_name                  STRING      COMMENT '车型名称',
    stat_date                   STRING      COMMENT '统计日期',
    total_alarm_cnt             INT         COMMENT '总报警次数',
    level_1_alarm_cnt           INT         COMMENT '一般报警次数',
    level_2_alarm_cnt           INT         COMMENT '较重报警次数',
    level_3_alarm_cnt           INT         COMMENT '严重报警次数',
    safety_related_alarm_cnt    INT         COMMENT '安全相关报警次数',
    battery_fault_cnt           INT         COMMENT '电池故障次数',
    motor_fault_cnt             INT         COMMENT '电机故障次数',
    vehicle_fault_cnt           INT         COMMENT '整车故障次数',
    other_fault_cnt             INT         COMMENT '其他故障次数',
    unique_fault_code_cnt       INT         COMMENT '不重复故障码数',
    alarm_duration_min          INT         COMMENT '报警持续时长（分钟）',
    first_alarm_time            TIMESTAMP   COMMENT '首次报警时间',
    last_alarm_time             TIMESTAMP   COMMENT '最后报警时间',
    workorder_created_cnt       INT         COMMENT '生成工单数'
)
COMMENT '车辆报警统计日表（DWS层）'
PARTITIONED BY (dt STRING)
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY');
