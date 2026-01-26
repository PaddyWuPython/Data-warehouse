CREATE TABLE IF NOT EXISTS dws_user_safety_complaint_1d (
    stat_date                   STRING      COMMENT '统计日期',
    total_complaint_cnt         INT         COMMENT '总投诉数',
    new_complaint_cnt           INT         COMMENT '新增投诉数',
    closed_complaint_cnt        INT         COMMENT '关闭投诉数',
    pending_complaint_cnt       INT         COMMENT '待处理投诉数',
    brake_fail_cnt              INT         COMMENT '刹车失灵投诉数',
    fire_cnt                    INT         COMMENT '自燃投诉数',
    noise_cnt                   INT         COMMENT '异响投诉数',
    battery_decay_cnt           INT         COMMENT '电池衰减投诉数',
    other_cnt                   INT         COMMENT '其他投诉数',
    high_severity_cnt           INT         COMMENT '高严重程度投诉数',
    medium_severity_cnt         INT         COMMENT '中严重程度投诉数',
    low_severity_cnt            INT         COMMENT '低严重程度投诉数',
    quality_issue_confirm_cnt   INT         COMMENT '确认质量问题数',
    quality_issue_rate          DOUBLE      COMMENT '质量问题确认率（%）',
    avg_response_lag_hour       DOUBLE      COMMENT '平均响应时长（小时）',
    response_sla_compliance_cnt INT         COMMENT '响应SLA达标数（<2小时）',
    response_sla_rate           DOUBLE      COMMENT '响应SLA达标率（%）',
    avg_handle_duration_day     DOUBLE      COMMENT '平均处理时长（天）',
    handle_sla_compliance_cnt   INT         COMMENT '处理SLA达标数（<7天）',
    handle_sla_rate             DOUBLE      COMMENT '处理SLA达标率（%）',
    avg_satisfaction_score      DOUBLE      COMMENT '平均满意度评分',
    high_satisfaction_cnt       INT         COMMENT '高满意度数（>=4分）',
    satisfaction_rate           DOUBLE      COMMENT '满意度达标率（%）',
    escalated_cnt               INT         COMMENT '升级投诉数',
    escalation_rate             DOUBLE      COMMENT '升级率（%）',
    compensation_cnt            INT         COMMENT '补偿数',
    total_compensation_amount   DOUBLE      COMMENT '总补偿金额（元）'
)
COMMENT '用户投诉统计日表（DWS层）'
PARTITIONED BY (dt STRING)
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY');
