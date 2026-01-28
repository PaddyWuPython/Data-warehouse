DROP TABLE IF EXISTS ads_model_safety_benchmark;

CREATE TABLE IF NOT EXISTS ads_model_safety_benchmark (
    model_name                  STRING      COMMENT '车型名称',
    stat_period                 STRING      COMMENT '统计周期（最近30天）',
    period_start_date           STRING      COMMENT '周期起始日期',
    period_end_date             STRING      COMMENT '周期结束日期',
    vehicle_cnt                 INT         COMMENT '车辆数',

    -- 行车安全
    avg_mileage_per_vehicle     DOUBLE      COMMENT '车均里程（km）',
    total_mileage               DOUBLE      COMMENT '总里程（km）',
    risk_event_cnt              INT         COMMENT '风险事件数',
    risk_event_per_1000km       DOUBLE      COMMENT '千公里风险事件率',
    avg_driving_score           DOUBLE      COMMENT '平均驾驶评分',

    -- 电池安全
    battery_alarm_vehicle_cnt   INT         COMMENT '电池报警车辆数',
    battery_alarm_rate          DOUBLE      COMMENT '电池报警率（%）',
    avg_voltage_diff            DOUBLE      COMMENT '平均压差（V）',
    thermal_risk_vehicle_cnt    INT         COMMENT '热失控风险车辆数',
    thermal_risk_rate           DOUBLE      COMMENT '热失控风险率（%）',
    avg_soh                     DOUBLE      COMMENT '平均SOH（%）',

    -- 充电安全
    charge_session_cnt          INT         COMMENT '充电次数',
    charge_abnormal_cnt         INT         COMMENT '充电异常次数',
    charge_abnormal_rate        DOUBLE      COMMENT '充电异常率（%）',
    avg_charge_temp             DOUBLE      COMMENT '平均充电温度（℃）',

    -- 报警响应
    alarm_vehicle_cnt           INT         COMMENT '报警车辆数',
    alarm_rate                  DOUBLE      COMMENT '报警率（%）',
    level_3_alarm_cnt           INT         COMMENT '严重报警次数',
    level_3_alarm_per_vehicle   DOUBLE      COMMENT '车均严重报警次数',

    -- 用户投诉
    complaint_cnt               INT         COMMENT '投诉数',
    complaint_per_1000_vehicle  DOUBLE      COMMENT '千车投诉率',
    quality_complaint_cnt       INT         COMMENT '质量投诉数',
    quality_complaint_rate      DOUBLE      COMMENT '质量投诉占比（%）',

    -- 综合评分
    comprehensive_safety_score  DOUBLE      COMMENT '综合安全评分（0-100）',
    safety_rank                 INT         COMMENT '安全排名（1=最优）'
)
COMMENT 'ADS层-车型安全质量对比分析（不分区）'
STORED AS TEXTFILE;

INSERT OVERWRITE TABLE ads_model_safety_benchmark
WITH dr AS (
  SELECT vin, model_name, total_mileage
  FROM dws_drive_vehicle_running_1d
  WHERE dt BETWEEN '${hiveconf:period_start_date}' AND '${hiveconf:period_end_date}'
),
db AS (
  SELECT vin, total_risk_event_cnt, driving_score, stat_date
  FROM dws_drive_vehicle_behavior_1d
  WHERE dt BETWEEN '${hiveconf:period_start_date}' AND '${hiveconf:period_end_date}'
),
bh AS (
  SELECT vin, avg_voltage_diff, is_thermal_risk, avg_soh, stat_date
  FROM dws_batt_vehicle_health_1d
  WHERE dt BETWEEN '${hiveconf:period_start_date}' AND '${hiveconf:period_end_date}'
),
cv AS (
  SELECT vin, charge_session_cnt, abnormal_session_cnt, abnormal_rate, avg_charging_power, max_temp_during_charge, stat_date
  FROM dws_chg_vehicle_charging_1d
  WHERE dt BETWEEN '${hiveconf:period_start_date}' AND '${hiveconf:period_end_date}'
),
va AS (
  SELECT vin, total_alarm_cnt, level_3_alarm_cnt, stat_date
  FROM dws_safe_vehicle_alarm_1d
  WHERE dt BETWEEN '${hiveconf:period_start_date}' AND '${hiveconf:period_end_date}'
),
uc AS (
  -- 投诉按“日表整体”没有车型维度；若你有投诉明细含车型，请替换这里
  SELECT
    '${hiveconf:period_end_date}' AS stat_date,
    CAST(NULL AS STRING) AS model_name,
    CAST(0 AS INT) AS complaint_cnt,
    CAST(0 AS INT) AS quality_complaint_cnt
)
, by_model AS (
  SELECT
    dr.model_name,
    COUNT(DISTINCT dr.vin)                                         AS vehicle_cnt,
    SUM(dr.total_mileage)                                          AS total_mileage,
    SUM(dr.total_mileage)/NULLIF(COUNT(DISTINCT dr.vin),0)          AS avg_mileage_per_vehicle,

    SUM(COALESCE(db.total_risk_event_cnt,0))                        AS risk_event_cnt,
    SUM(COALESCE(db.total_risk_event_cnt,0))/NULLIF(SUM(dr.total_mileage),0)*1000
                                                                     AS risk_event_per_1000km,
    AVG(COALESCE(db.driving_score,100))                             AS avg_driving_score,

    -- 电池
    SUM(CASE WHEN va.total_alarm_cnt>0 THEN 1 ELSE 0 END)           AS battery_alarm_vehicle_cnt, -- 近似：报警车
    SUM(CASE WHEN va.total_alarm_cnt>0 THEN 1 ELSE 0 END)/NULLIF(COUNT(DISTINCT dr.vin),0)*100
                                                                     AS battery_alarm_rate,
    AVG(bh.avg_voltage_diff)                                        AS avg_voltage_diff,
    SUM(CASE WHEN bh.is_thermal_risk=1 THEN 1 ELSE 0 END)           AS thermal_risk_vehicle_cnt,
    SUM(CASE WHEN bh.is_thermal_risk=1 THEN 1 ELSE 0 END)/NULLIF(COUNT(DISTINCT dr.vin),0)*100
                                                                     AS thermal_risk_rate,
    AVG(bh.avg_soh)                                                 AS avg_soh,

    -- 充电
    SUM(COALESCE(cv.charge_session_cnt,0))                          AS charge_session_cnt,
    SUM(COALESCE(cv.abnormal_session_cnt,0))                        AS charge_abnormal_cnt,
    SUM(COALESCE(cv.abnormal_session_cnt,0))/NULLIF(SUM(COALESCE(cv.charge_session_cnt,0)),0)*100
                                                                     AS charge_abnormal_rate,
    AVG(cv.max_temp_during_charge)                                  AS avg_charge_temp,

    -- 报警
    COUNT(DISTINCT CASE WHEN va.total_alarm_cnt>0 THEN dr.vin END)   AS alarm_vehicle_cnt,
    COUNT(DISTINCT CASE WHEN va.total_alarm_cnt>0 THEN dr.vin END)/NULLIF(COUNT(DISTINCT dr.vin),0)*100
                                                                     AS alarm_rate,
    SUM(COALESCE(va.level_3_alarm_cnt,0))                            AS level_3_alarm_cnt,
    SUM(COALESCE(va.level_3_alarm_cnt,0))/NULLIF(COUNT(DISTINCT dr.vin),0)
                                                                     AS level_3_alarm_per_vehicle,

    -- 投诉（占位：如有车型投诉明细可替换）
    0 AS complaint_cnt,
    0 AS quality_complaint_cnt,
    0 AS complaint_per_1000_vehicle,
    0 AS quality_complaint_rate
  FROM dr
  LEFT JOIN db ON dr.vin=db.vin AND db.stat_date=dr.dt
  LEFT JOIN bh ON dr.vin=bh.vin AND bh.stat_date=dr.dt
  LEFT JOIN cv ON dr.vin=cv.vin AND cv.stat_date=dr.dt
  LEFT JOIN va ON dr.vin=va.vin AND va.stat_date=dr.dt
  GROUP BY dr.model_name
),
scored AS (
  SELECT
    *,
    -- 示例综合评分：可按你们指标体系调权
    (100
      - risk_event_per_1000km * 2
      - thermal_risk_rate * 1.5
      - charge_abnormal_rate * 0.8
      - alarm_rate * 1.0
    ) AS comprehensive_safety_score
  FROM by_model
)
SELECT
  model_name,
  '${hiveconf:stat_period}'          AS stat_period,
  '${hiveconf:period_start_date}'    AS period_start_date,
  '${hiveconf:period_end_date}'      AS period_end_date,
  vehicle_cnt,

  avg_mileage_per_vehicle,
  total_mileage,
  risk_event_cnt,
  risk_event_per_1000km,
  avg_driving_score,

  battery_alarm_vehicle_cnt,
  battery_alarm_rate,
  avg_voltage_diff,
  thermal_risk_vehicle_cnt,
  thermal_risk_rate,
  avg_soh,

  charge_session_cnt,
  charge_abnormal_cnt,
  charge_abnormal_rate,
  avg_charge_temp,

  alarm_vehicle_cnt,
  alarm_rate,
  level_3_alarm_cnt,
  level_3_alarm_per_vehicle,

  complaint_cnt,
  complaint_per_1000_vehicle,
  quality_complaint_cnt,
  quality_complaint_rate,

  comprehensive_safety_score,
  DENSE_RANK() OVER (ORDER BY comprehensive_safety_score DESC) AS safety_rank
FROM scored;