CREATE TABLE IF NOT EXISTS dws_safe_alarm_response_1d (
    stat_date                   STRING      COMMENT '统计日期',
    total_workorder_cnt         INT         COMMENT '总工单数',
    new_workorder_cnt           INT         COMMENT '新增工单数',
    closed_workorder_cnt        INT         COMMENT '关闭工单数',
    pending_workorder_cnt       INT         COMMENT '待处理工单数',
    high_priority_cnt           INT         COMMENT '高优先级工单数',
    avg_response_lag_sec        DOUBLE      COMMENT '平均响应时长（秒）',
    median_response_lag_sec     DOUBLE      COMMENT '中位数响应时长',
    response_sla_compliance_cnt INT         COMMENT '响应SLA达标数（<5分钟）',
    response_sla_rate           DOUBLE      COMMENT '响应SLA达标率（%）',
    avg_handle_duration_hour    DOUBLE      COMMENT '平均处理时长（小时）',
    handle_sla_compliance_cnt   INT         COMMENT '处理SLA达标数（<24小时）',
    handle_sla_rate             DOUBLE      COMMENT '处理SLA达标率（%）',
    timeout_cnt                 INT         COMMENT '超时工单数',
    resolved_cnt                INT         COMMENT '已解决工单数',
    resolve_rate                DOUBLE      COMMENT '解决率（%）',
    false_alarm_cnt             INT         COMMENT '误报工单数',
    false_alarm_rate            DOUBLE      COMMENT '误报率（%）',
    remote_guide_cnt            INT         COMMENT '远程指导数',
    onsite_rescue_cnt           INT         COMMENT '现场救援数',
    avg_feedback_score          DOUBLE      COMMENT '平均用户评分',
    high_score_cnt              INT         COMMENT '高分评价数（>=4分）',
    satisfaction_rate           DOUBLE      COMMENT '满意度（%）'
)
COMMENT '报警处置效率统计日表（DWS层）'
PARTITIONED BY (dt STRING)
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY');
