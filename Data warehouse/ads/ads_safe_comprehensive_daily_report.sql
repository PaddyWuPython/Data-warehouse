DROP TABLE IF EXISTS ads_safe_comprehensive_daily_report;

CREATE TABLE IF NOT EXISTS ads_safe_comprehensive_daily_report (
    -- 时间维度
    stat_date                   STRING      COMMENT '统计日期',
    report_generate_time        TIMESTAMP   COMMENT '报表生成时间',
    
    -- ========== 行车安全域 ==========
    driving_active_vehicle_cnt  INT         COMMENT '行车活跃车辆数',
    driving_total_mileage       DOUBLE      COMMENT '总里程（km）',
    driving_total_risk_event_cnt INT        COMMENT '总风险事件数',
    driving_risk_event_per_10k_km DOUBLE    COMMENT '万公里风险事件率',
    driving_avg_score           DOUBLE      COMMENT '平均驾驶评分',
    driving_accident_cnt        INT         COMMENT '事故次数',
    
    -- ========== 电池安全域 ==========
    battery_monitored_vehicle_cnt INT       COMMENT '电池监控车辆数',
    battery_abnormal_vehicle_cnt INT        COMMENT '电池异常车辆数',
    battery_thermal_risk_cnt    INT         COMMENT '热失控风险车辆数',
    battery_avg_max_temp        DOUBLE      COMMENT '平均最高温度（℃）',
    battery_avg_voltage_diff    DOUBLE      COMMENT '平均压差（V）',
    battery_over_temp_cnt       INT         COMMENT '过温次数',
    battery_avg_soh             DOUBLE      COMMENT '平均SOH（%）',
    
    -- ========== 充电安全域 ==========
    charging_total_session_cnt  INT         COMMENT '总充电次数',
    charging_abnormal_cnt       INT         COMMENT '充电异常次数',
    charging_abnormal_rate      DOUBLE      COMMENT '充电异常率（%）',
    charging_total_energy       DOUBLE      COMMENT '总充电电量（kWh）',
    charging_over_temp_cnt      INT         COMMENT '充电过温次数',
    charging_avg_power          DOUBLE      COMMENT '平均充电功率（kW）',
    
    -- ========== 报警响应域 ==========
    alarm_total_cnt             INT         COMMENT '总报警次数',
    alarm_vehicle_cnt           INT         COMMENT '报警车辆数',
    alarm_level_3_cnt           INT         COMMENT '严重报警次数',
    alarm_safety_related_cnt    INT         COMMENT '安全相关报警次数',
    alarm_avg_response_time_sec DOUBLE      COMMENT '平均响应时间（秒）',
    alarm_response_sla_rate     DOUBLE      COMMENT '响应SLA达标率（%）',
    alarm_workorder_cnt         INT         COMMENT '工单数',
    alarm_resolve_rate          DOUBLE      COMMENT '工单解决率（%）',
    alarm_avg_user_score        DOUBLE      COMMENT '平均用户评分',
    
    -- ========== 用户安全域 ==========
    complaint_new_cnt           INT         COMMENT '新增投诉数',
    complaint_safety_cnt        INT         COMMENT '安全类投诉数',
    complaint_quality_confirm_cnt INT       COMMENT '质量问题确认数',
    complaint_avg_response_hour DOUBLE      COMMENT '平均投诉响应时长（小时）',
    complaint_satisfaction_score DOUBLE     COMMENT '投诉满意度评分',
    complaint_escalation_cnt    INT         COMMENT '升级投诉数',
    
    -- ========== 综合评估 ==========
    overall_safety_score        DOUBLE      COMMENT '综合安全评分（0-100）',
    risk_level                  STRING      COMMENT '综合风险等级：LOW/MEDIUM/HIGH',
    key_risk_items              STRING      COMMENT '关键风险项（JSON格式）'
)
COMMENT 'ADS层-综合安全日报（不分区，行式存储）'
STORED AS TEXTFILE;
