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

INSERT OVERWRITE TABLE ads_safe_comprehensive_daily_report
SELECT
    '${hiveconf:etl_date}' AS stat_date,
    CURRENT_TIMESTAMP()    AS report_generate_time,

    -- 行车安全域（从DWS聚合）
    SUM(CASE WHEN dr.total_mileage > 0 THEN 1 ELSE 0 END)                        AS driving_active_vehicle_cnt,
    SUM(dr.total_mileage)                                                        AS driving_total_mileage,
    SUM(COALESCE(db.total_risk_event_cnt, 0))                                    AS driving_total_risk_event_cnt,
    SUM(COALESCE(db.total_risk_event_cnt, 0)) / NULLIF(SUM(dr.total_mileage), 0) * 10000
                                                                                 AS driving_risk_event_per_10k_km,
    AVG(COALESCE(db.driving_score, 100))                                         AS driving_avg_score,
    0                                                                            AS driving_accident_cnt, -- 如有事故表可替换

    -- 电池安全域
    COUNT(DISTINCT bh.vin)                                                       AS battery_monitored_vehicle_cnt,
    SUM(CASE WHEN COALESCE(ba.total_abnormal_duration_min, 0) > 0 THEN 1 ELSE 0 END)
                                                                                 AS battery_abnormal_vehicle_cnt,
    SUM(CASE WHEN bh.is_thermal_risk = 1 THEN 1 ELSE 0 END)                      AS battery_thermal_risk_cnt,
    AVG(bh.avg_max_temp)                                                         AS battery_avg_max_temp,
    AVG(bh.avg_voltage_diff)                                                     AS battery_avg_voltage_diff,
    SUM(bh.over_temp_cnt)                                                        AS battery_over_temp_cnt,
    AVG(bh.avg_soh)                                                              AS battery_avg_soh,

    -- 充电安全域
    SUM(cv.charge_session_cnt)                                                   AS charging_total_session_cnt,
    SUM(cv.abnormal_session_cnt)                                                 AS charging_abnormal_cnt,
    SUM(cv.abnormal_session_cnt) / NULLIF(SUM(cv.charge_session_cnt), 0) * 100   AS charging_abnormal_rate,
    SUM(cv.total_charged_energy)                                                 AS charging_total_energy,
    SUM(cv.over_temp_cnt)                                                        AS charging_over_temp_cnt,
    AVG(cv.avg_charging_power)                                                   AS charging_avg_power,

    -- 报警响应域
    SUM(va.total_alarm_cnt)                                                      AS alarm_total_cnt,
    COUNT(DISTINCT va.vin)                                                       AS alarm_vehicle_cnt,
    SUM(va.level_3_alarm_cnt)                                                    AS alarm_level_3_cnt,
    SUM(va.safety_related_alarm_cnt)                                             AS alarm_safety_related_cnt,
    (SELECT avg_response_lag_sec FROM dws_safe_alarm_response_1d WHERE dt='${hiveconf:etl_date}')
                                                                                 AS alarm_avg_response_time_sec,
    (SELECT response_sla_rate FROM dws_safe_alarm_response_1d WHERE dt='${hiveconf:etl_date}')
                                                                                 AS alarm_response_sla_rate,
    (SELECT total_workorder_cnt FROM dws_safe_alarm_response_1d WHERE dt='${hiveconf:etl_date}')
                                                                                 AS alarm_workorder_cnt,
    (SELECT resolve_rate FROM dws_safe_alarm_response_1d WHERE dt='${hiveconf:etl_date}')
                                                                                 AS alarm_resolve_rate,
    (SELECT avg_feedback_score FROM dws_safe_alarm_response_1d WHERE dt='${hiveconf:etl_date}')
                                                                                 AS alarm_avg_user_score,

    -- 用户安全域
    (SELECT new_complaint_cnt FROM dws_user_safety_complaint_1d WHERE dt='${hiveconf:etl_date}')
                                                                                 AS complaint_new_cnt,
    (SELECT brake_fail_cnt + fire_cnt FROM dws_user_safety_complaint_1d WHERE dt='${hiveconf:etl_date}')
                                                                                 AS complaint_safety_cnt,
    (SELECT quality_issue_confirm_cnt FROM dws_user_safety_complaint_1d WHERE dt='${hiveconf:etl_date}')
                                                                                 AS complaint_quality_confirm_cnt,
    (SELECT avg_response_lag_hour FROM dws_user_safety_complaint_1d WHERE dt='${hiveconf:etl_date}')
                                                                                 AS complaint_avg_response_hour,
    (SELECT avg_satisfaction_score FROM dws_user_safety_complaint_1d WHERE dt='${hiveconf:etl_date}')
                                                                                 AS complaint_satisfaction_score,
    (SELECT escalated_cnt FROM dws_user_safety_complaint_1d WHERE dt='${hiveconf:etl_date}')
                                                                                 AS complaint_escalation_cnt,

    -- 综合评估（示例：可按你们指标体系替换权重）
    (100
      - (SUM(va.level_3_alarm_cnt) / NULLIF(COUNT(DISTINCT dr.vin), 0) * 10)
      - (SUM(CASE WHEN bh.is_thermal_risk=1 THEN 1 ELSE 0 END) / NULLIF(COUNT(DISTINCT bh.vin), 0) * 20)
      - ((SELECT escalation_rate FROM dws_user_safety_complaint_1d WHERE dt='${hiveconf:etl_date}') * 5)
    )                                                                            AS overall_safety_score,

    CASE
      WHEN (100
              - (SUM(va.level_3_alarm_cnt) / NULLIF(COUNT(DISTINCT dr.vin), 0) * 10)
              - (SUM(CASE WHEN bh.is_thermal_risk=1 THEN 1 ELSE 0 END) / NULLIF(COUNT(DISTINCT bh.vin), 0) * 20)
              - ((SELECT escalation_rate FROM dws_user_safety_complaint_1d WHERE dt='${hiveconf:etl_date}') * 5)
           ) >= 90 THEN 'LOW'
      WHEN (100
              - (SUM(va.level_3_alarm_cnt) / NULLIF(COUNT(DISTINCT dr.vin), 0) * 10)
              - (SUM(CASE WHEN bh.is_thermal_risk=1 THEN 1 ELSE 0 END) / NULLIF(COUNT(DISTINCT bh.vin), 0) * 20)
              - ((SELECT escalation_rate FROM dws_user_safety_complaint_1d WHERE dt='${hiveconf:etl_date}') * 5)
           ) >= 70 THEN 'MEDIUM'
      ELSE 'HIGH'
    END                                                                          AS risk_level,

    CONCAT(
      '{"thermal_risk_cnt":', SUM(CASE WHEN bh.is_thermal_risk=1 THEN 1 ELSE 0 END),
      ',"level_3_alarm_cnt":', SUM(va.level_3_alarm_cnt),
      ',"complaint_escalation":', (SELECT escalated_cnt FROM dws_user_safety_complaint_1d WHERE dt='${hiveconf:etl_date}'),
      '}'
    )                                                                            AS key_risk_items
FROM
    dws_drive_vehicle_running_1d dr
LEFT JOIN dws_drive_vehicle_behavior_1d db
    ON dr.vin = db.vin AND dr.stat_date = db.stat_date AND db.dt='${hiveconf:etl_date}'
LEFT JOIN dws_batt_vehicle_health_1d bh
    ON dr.vin = bh.vin AND dr.stat_date = bh.stat_date AND bh.dt='${hiveconf:etl_date}'
LEFT JOIN dws_batt_vehicle_abnormal_1d ba
    ON dr.vin = ba.vin AND dr.stat_date = ba.stat_date AND ba.dt='${hiveconf:etl_date}'
LEFT JOIN dws_chg_vehicle_charging_1d cv
    ON dr.vin = cv.vin AND dr.stat_date = cv.stat_date AND cv.dt='${hiveconf:etl_date}'
LEFT JOIN dws_safe_vehicle_alarm_1d va
    ON dr.vin = va.vin AND dr.stat_date = va.stat_date AND va.dt='${hiveconf:etl_date}'
WHERE
    dr.dt='${hiveconf:etl_date}';
