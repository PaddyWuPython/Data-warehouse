DROP TABLE IF EXISTS ads_safe_alarm_response_monthly;

CREATE TABLE IF NOT EXISTS ads_safe_alarm_response_monthly (
    stat_month                   STRING      COMMENT '统计月份（yyyy-MM）',
    month_start_date             STRING      COMMENT '月起始日期',
    month_end_date               STRING      COMMENT '月结束日期',
    report_generate_time         TIMESTAMP   COMMENT '报表生成时间',

    -- 报警量指标
    total_alarm_cnt              INT         COMMENT '总报警次数',
    alarm_vehicle_cnt            INT         COMMENT '报警车辆数',
    total_vehicle_cnt            INT         COMMENT '总车辆数',
    alarm_vehicle_rate           DOUBLE      COMMENT '报警车辆率（%）',
    level_3_alarm_cnt            INT         COMMENT '严重报警次数',
    level_3_alarm_rate           DOUBLE      COMMENT '严重报警占比（%）',

    -- 响应效率
    total_workorder_cnt          INT         COMMENT '总工单数',
    avg_response_time_min        DOUBLE      COMMENT '平均响应时间（分钟）',
    median_response_time_min     DOUBLE      COMMENT '中位数响应时间（分钟）',
    response_sla_compliance_rate DOUBLE      COMMENT '响应SLA达标率（%，<5分钟）',
    avg_handle_duration_hour     DOUBLE      COMMENT '平均处理时长（小时）',
    handle_sla_compliance_rate   DOUBLE      COMMENT '处理SLA达标率（%，<24小时）',

    -- 处理质量
    resolve_rate                 DOUBLE      COMMENT '解决率（%）',
    false_alarm_rate             DOUBLE      COMMENT '误报率（%）',
    avg_user_score               DOUBLE      COMMENT '平均用户评分',
    high_score_rate              DOUBLE      COMMENT '高分评价率（%，>=4分）',

    -- 故障分析（JSON）
    top_fault_codes              STRING      COMMENT '高频故障码TOP10（JSON）',
    fault_category_distribution  STRING      COMMENT '故障类别分布（JSON）',
    fault_recurrence_rate        DOUBLE      COMMENT '故障复发率（%）',

    -- 环比变化
    mom_alarm_cnt_change_rate    DOUBLE      COMMENT '报警次数环比（%）',
    mom_response_sla_change      DOUBLE      COMMENT '响应SLA环比变化（百分点）'
)
COMMENT 'ADS层-报警响应绩效月报（不分区）'
STORED AS TEXTFILE;

INSERT OVERWRITE TABLE ads_safe_alarm_response_monthly
WITH va AS (
  SELECT *
  FROM dws_safe_vehicle_alarm_1d
  WHERE dt BETWEEN '${hiveconf:month_start_date}' AND '${hiveconf:month_end_date}'
),
resp AS (
  SELECT *
  FROM dws_safe_alarm_response_1d
  WHERE dt BETWEEN '${hiveconf:month_start_date}' AND '${hiveconf:month_end_date}'
),
agg AS (
  SELECT
    SUM(total_alarm_cnt)                         AS total_alarm_cnt,
    COUNT(DISTINCT vin)                          AS alarm_vehicle_cnt,
    SUM(level_3_alarm_cnt)                       AS level_3_alarm_cnt,
    SUM(level_3_alarm_cnt)/NULLIF(SUM(total_alarm_cnt),0)*100
                                                 AS level_3_alarm_rate
  FROM va
),
resp_agg AS (
  SELECT
    SUM(total_workorder_cnt)                     AS total_workorder_cnt,
    AVG(avg_response_lag_sec)/60.0               AS avg_response_time_min,
    percentile_approx(avg_response_lag_sec, 0.5)/60.0
                                                 AS median_response_time_min,
    AVG(response_sla_rate)                       AS response_sla_compliance_rate,
    AVG(avg_handle_duration_hour)                AS avg_handle_duration_hour,
    AVG(handle_sla_rate)                         AS handle_sla_compliance_rate,
    AVG(resolve_rate)                            AS resolve_rate,
    AVG(false_alarm_rate)                        AS false_alarm_rate,
    AVG(avg_feedback_score)                      AS avg_user_score,
    -- 这里用“高分评价数/总评价数”需要明细；若无明细可先近似为 NULL 或 0
    CAST(NULL AS DOUBLE)                         AS high_score_rate
  FROM resp
),
dim_vehicle AS (
  -- 若你有 DIM 车辆主表，建议换成 dim_vehicle_zip/full；这里用行车DWS当月出现过的车近似总车辆数
  SELECT COUNT(DISTINCT vin) AS total_vehicle_cnt
  FROM dws_drive_vehicle_running_1d
  WHERE dt BETWEEN '${hiveconf:month_start_date}' AND '${hiveconf:month_end_date}'
),
json_fault AS (
  -- 示例：故障码Top10需要报警明细表；当前 DWS 仅有 unique_fault_code_cnt，无法还原Top10
  SELECT CAST(NULL AS STRING) AS top_fault_codes
),
json_cat AS (
  -- 示例：类别分布同理；若你有 DWD 故障明细，可按类别聚合转JSON
  SELECT CAST(NULL AS STRING) AS fault_category_distribution
),
prev AS (
  SELECT
    SUM(total_alarm_cnt) AS prev_alarm_cnt,
    AVG(response_sla_rate) AS prev_response_sla
  FROM dws_safe_alarm_response_1d
  WHERE dt BETWEEN ADD_MONTHS('${hiveconf:month_start_date}', -1) AND ADD_MONTHS('${hiveconf:month_end_date}', -1)
)
SELECT
  '${hiveconf:stat_month}'            AS stat_month,
  '${hiveconf:month_start_date}'      AS month_start_date,
  '${hiveconf:month_end_date}'        AS month_end_date,
  CURRENT_TIMESTAMP()                 AS report_generate_time,

  a.total_alarm_cnt,
  a.alarm_vehicle_cnt,
  v.total_vehicle_cnt,
  a.alarm_vehicle_cnt/NULLIF(v.total_vehicle_cnt,0)*100  AS alarm_vehicle_rate,
  a.level_3_alarm_cnt,
  a.level_3_alarm_rate,

  r.total_workorder_cnt,
  r.avg_response_time_min,
  r.median_response_time_min,
  r.response_sla_compliance_rate,
  r.avg_handle_duration_hour,
  r.handle_sla_compliance_rate,

  r.resolve_rate,
  r.false_alarm_rate,
  r.avg_user_score,
  r.high_score_rate,

  (SELECT top_fault_codes FROM json_fault)               AS top_fault_codes,
  (SELECT fault_category_distribution FROM json_cat)     AS fault_category_distribution,
  CAST(NULL AS DOUBLE)                                   AS fault_recurrence_rate, -- 需工单/故障复发明细

  (a.total_alarm_cnt - p.prev_alarm_cnt)/NULLIF(p.prev_alarm_cnt,0)*100
                                                         AS mom_alarm_cnt_change_rate,
  (r.response_sla_compliance_rate - p.prev_response_sla)  AS mom_response_sla_change
FROM agg a
CROSS JOIN resp_agg r
CROSS JOIN dim_vehicle v
CROSS JOIN prev p;