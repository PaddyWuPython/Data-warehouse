DROP TABLE IF EXISTS ads_user_complaint_analysis_monthly;

CREATE TABLE IF NOT EXISTS ads_user_complaint_analysis_monthly (
    stat_month                    STRING      COMMENT '统计月份',
    report_generate_time          TIMESTAMP   COMMENT '报表生成时间',

    -- 投诉量
    total_complaint_cnt           INT         COMMENT '总投诉数',
    safety_complaint_cnt          INT         COMMENT '安全类投诉数',
    safety_complaint_rate         DOUBLE      COMMENT '安全投诉占比（%）',
    thousand_vehicle_complaint_rate DOUBLE    COMMENT '千车投诉率',

    -- 投诉类型分布（JSON）
    complaint_type_distribution   STRING      COMMENT '投诉类型TOP5分布（JSON）',

    -- 处理时效
    avg_response_time_hour        DOUBLE      COMMENT '平均响应时间（小时）',
    avg_handle_time_day           DOUBLE      COMMENT '平均处理时长（天）',
    response_sla_rate             DOUBLE      COMMENT '响应SLA达标率（<2小时）',
    close_sla_rate                DOUBLE      COMMENT '关闭SLA达标率（<7天）',

    -- 处理质量
    quality_issue_confirm_rate    DOUBLE      COMMENT '质量问题确认率（%）',
    compensation_ratio            DOUBLE      COMMENT '补偿率（%）',
    avg_compensation_amount       DOUBLE      COMMENT '平均补偿金额（元）',
    avg_satisfaction_score        DOUBLE      COMMENT '平均满意度评分',
    satisfaction_rate             DOUBLE      COMMENT '满意度达标率（>=4分）',
    escalation_rate               DOUBLE      COMMENT '升级率（%）',

    -- 区域分布（JSON）
    complaint_by_region           STRING      COMMENT '投诉区域分布TOP10（JSON）',
    complaint_by_model            STRING      COMMENT '投诉车型分布TOP10（JSON）',

    -- 环比变化
    mom_complaint_cnt_change_rate DOUBLE      COMMENT '投诉数环比（%）',
    mom_satisfaction_change       DOUBLE      COMMENT '满意度环比变化（分）'
)
COMMENT 'ADS层-用户投诉分析月报（不分区）'
STORED AS TEXTFILE;

INSERT OVERWRITE TABLE ads_user_complaint_analysis_monthly
WITH c AS (
  SELECT *
  FROM dws_user_safety_complaint_1d
  WHERE dt BETWEEN '${hiveconf:month_start_date}' AND '${hiveconf:month_end_date}'
),
v AS (
  -- 总车辆数口径同上：建议换成 dim_vehicle
  SELECT COUNT(DISTINCT vin) AS total_vehicle_cnt
  FROM dws_drive_vehicle_running_1d
  WHERE dt BETWEEN '${hiveconf:month_start_date}' AND '${hiveconf:month_end_date}'
),
agg AS (
  SELECT
    SUM(total_complaint_cnt)                         AS total_complaint_cnt,
    SUM(brake_fail_cnt + fire_cnt)                   AS safety_complaint_cnt,
    SUM(brake_fail_cnt + fire_cnt)/NULLIF(SUM(total_complaint_cnt),0)*100
                                                     AS safety_complaint_rate,
    AVG(avg_response_lag_hour)                       AS avg_response_time_hour,
    AVG(avg_handle_duration_day)                     AS avg_handle_time_day,
    AVG(response_sla_rate)                           AS response_sla_rate,
    AVG(handle_sla_rate)                             AS close_sla_rate,
    SUM(quality_issue_confirm_cnt)/NULLIF(SUM(total_complaint_cnt),0)*100
                                                     AS quality_issue_confirm_rate,
    SUM(compensation_cnt)/NULLIF(SUM(total_complaint_cnt),0)*100
                                                     AS compensation_ratio,
    SUM(total_compensation_amount)/NULLIF(SUM(compensation_cnt),0)
                                                     AS avg_compensation_amount,
    AVG(avg_satisfaction_score)                      AS avg_satisfaction_score,
    AVG(satisfaction_rate)                           AS satisfaction_rate,
    SUM(escalated_cnt)/NULLIF(SUM(total_complaint_cnt),0)*100
                                                     AS escalation_rate
  FROM c
),
json_type AS (
  -- DWS日表并未给出“投诉类型Top5”，这里先置空；如你有投诉明细表，可在此聚合生成JSON
  SELECT CAST(NULL AS STRING) AS complaint_type_distribution
),
json_region AS (
  SELECT CAST(NULL AS STRING) AS complaint_by_region
),
json_model AS (
  SELECT CAST(NULL AS STRING) AS complaint_by_model
),
prev AS (
  SELECT
    SUM(total_complaint_cnt) AS prev_total_complaint_cnt,
    AVG(avg_satisfaction_score) AS prev_avg_satisfaction_score
  FROM dws_user_safety_complaint_1d
  WHERE dt BETWEEN ADD_MONTHS('${hiveconf:month_start_date}', -1) AND ADD_MONTHS('${hiveconf:month_end_date}', -1)
)
SELECT
  '${hiveconf:stat_month}'        AS stat_month,
  CURRENT_TIMESTAMP()             AS report_generate_time,

  a.total_complaint_cnt,
  a.safety_complaint_cnt,
  a.safety_complaint_rate,
  a.total_complaint_cnt/NULLIF(v.total_vehicle_cnt,0)*1000 AS thousand_vehicle_complaint_rate,

  (SELECT complaint_type_distribution FROM json_type) AS complaint_type_distribution,

  a.avg_response_time_hour,
  a.avg_handle_time_day,
  a.response_sla_rate,
  a.close_sla_rate,

  a.quality_issue_confirm_rate,
  a.compensation_ratio,
  a.avg_compensation_amount,
  a.avg_satisfaction_score,
  a.satisfaction_rate,
  a.escalation_rate,

  (SELECT complaint_by_region FROM json_region) AS complaint_by_region,
  (SELECT complaint_by_model FROM json_model)   AS complaint_by_model,

  (a.total_complaint_cnt - p.prev_total_complaint_cnt)/NULLIF(p.prev_total_complaint_cnt,0)*100
                                         AS mom_complaint_cnt_change_rate,
  (a.avg_satisfaction_score - p.prev_avg_satisfaction_score)
                                         AS mom_satisfaction_change
FROM agg a
CROSS JOIN v
CROSS JOIN prev p;