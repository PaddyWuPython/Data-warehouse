DROP TABLE IF EXISTS ads_chg_quality_weekly_report;

CREATE TABLE IF NOT EXISTS ads_chg_quality_weekly_report (
    stat_week                   STRING      COMMENT '统计周（yyyy-Www）',
    week_start_date             STRING      COMMENT '周起始日期',
    week_end_date               STRING      COMMENT '周结束日期',
    report_generate_time        TIMESTAMP   COMMENT '报表生成时间',

    -- 充电量指标
    total_charge_session_cnt    INT         COMMENT '总充电次数',
    charge_vehicle_cnt          INT         COMMENT '充电车辆数',
    total_charged_energy        DOUBLE      COMMENT '总充电电量（kWh）',
    avg_session_energy          DOUBLE      COMMENT '平均单次充电量（kWh）',
    total_charge_duration_hour  DOUBLE      COMMENT '总充电时长（小时）',

    -- 质量指标
    abnormal_session_cnt        INT         COMMENT '异常中断次数',
    abnormal_rate               DOUBLE      COMMENT '异常率（%）',
    over_temp_session_cnt       INT         COMMENT '过温充电次数',
    over_current_session_cnt    INT         COMMENT '过流充电次数',

    -- 效率指标
    avg_charging_power          DOUBLE      COMMENT '平均充电功率（kW）',
    fast_charge_cnt             INT         COMMENT '快充次数',
    fast_charge_ratio           DOUBLE      COMMENT '快充占比（%）',
    avg_wait_duration_min       INT         COMMENT '平均等待时长（分钟）',

    -- 站点质量排名（JSON）
    top_abnormal_stations       STRING      COMMENT '异常次数最多站点TOP10（JSON）',
    top_quality_stations        STRING      COMMENT '质量最优站点TOP10（JSON）',

    -- 环比变化
    wow_charge_cnt_change_rate  DOUBLE      COMMENT '充电次数环比变化（%）',
    wow_abnormal_rate_change    DOUBLE      COMMENT '异常率环比变化（百分点）'
)
COMMENT 'ADS层-充电质量周报（不分区）'
STORED AS TEXTFILE;

INSERT OVERWRITE TABLE ads_chg_quality_weekly_report
WITH base AS (
  SELECT *
  FROM dws_chg_vehicle_charging_1d
  WHERE dt BETWEEN '${hiveconf:week_start_date}' AND '${hiveconf:week_end_date}'
),
agg AS (
  SELECT
    COUNT(*)                                            AS row_cnt,
    SUM(charge_session_cnt)                              AS total_charge_session_cnt,
    COUNT(DISTINCT vin)                                  AS charge_vehicle_cnt,
    SUM(total_charged_energy)                            AS total_charged_energy,
    AVG(avg_charged_energy)                              AS avg_session_energy,
    SUM(total_charge_duration_min)/60.0                  AS total_charge_duration_hour,

    SUM(abnormal_session_cnt)                            AS abnormal_session_cnt,
    SUM(abnormal_session_cnt)/NULLIF(SUM(charge_session_cnt),0)*100
                                                        AS abnormal_rate,
    SUM(over_temp_cnt)                                   AS over_temp_session_cnt,
    SUM(over_current_cnt)                                AS over_current_session_cnt,

    AVG(avg_charging_power)                              AS avg_charging_power,
    SUM(fast_charge_cnt)                                 AS fast_charge_cnt,
    SUM(fast_charge_cnt)/NULLIF(SUM(charge_session_cnt),0)*100
                                                        AS fast_charge_ratio,

    0                                                    AS avg_wait_duration_min -- 如有排队明细可替换
  FROM base
),
station_rank_abn AS (
  -- 若你有 dws_chg_station_operation_1d 可直接用；这里假设存在该表
  SELECT
    CONCAT(
      '[',
      CONCAT_WS(',',
        COLLECT_LIST(CONCAT('{"station_id":"', station_id, '","abnormal_cnt":', CAST(abnormal_session_cnt AS STRING), '}'))
      ),
      ']'
    ) AS json_arr
  FROM (
    SELECT station_id, SUM(abnormal_session_cnt) AS abnormal_session_cnt
    FROM dws_chg_station_operation_1d
    WHERE dt BETWEEN '${hiveconf:week_start_date}' AND '${hiveconf:week_end_date}'
    GROUP BY station_id
    ORDER BY abnormal_session_cnt DESC
    LIMIT 10
  ) t
),
station_rank_quality AS (
  SELECT
    CONCAT(
      '[',
      CONCAT_WS(',',
        COLLECT_LIST(CONCAT('{"station_id":"', station_id, '","abnormal_rate":', CAST(abnormal_rate AS STRING), '}'))
      ),
      ']'
    ) AS json_arr
  FROM (
    SELECT
      station_id,
      SUM(abnormal_session_cnt)/NULLIF(SUM(total_session_cnt),0)*100 AS abnormal_rate
    FROM dws_chg_station_operation_1d
    WHERE dt BETWEEN '${hiveconf:week_start_date}' AND '${hiveconf:week_end_date}'
    GROUP BY station_id
    ORDER BY abnormal_rate ASC
    LIMIT 10
  ) t
),
prev AS (
  -- 上一周窗口（示例：直接用日期减7天；生产环境建议用日历表）
  SELECT
    SUM(charge_session_cnt) AS prev_total_charge_session_cnt,
    SUM(abnormal_session_cnt)/NULLIF(SUM(charge_session_cnt),0)*100 AS prev_abnormal_rate
  FROM dws_chg_vehicle_charging_1d
  WHERE dt BETWEEN DATE_SUB('${hiveconf:week_start_date}', 7) AND DATE_SUB('${hiveconf:week_end_date}', 7)
)
SELECT
  '${hiveconf:stat_week}'          AS stat_week,
  '${hiveconf:week_start_date}'    AS week_start_date,
  '${hiveconf:week_end_date}'      AS week_end_date,
  CURRENT_TIMESTAMP()              AS report_generate_time,

  a.total_charge_session_cnt,
  a.charge_vehicle_cnt,
  a.total_charged_energy,
  a.avg_session_energy,
  a.total_charge_duration_hour,

  a.abnormal_session_cnt,
  a.abnormal_rate,
  a.over_temp_session_cnt,
  a.over_current_session_cnt,

  a.avg_charging_power,
  a.fast_charge_cnt,
  a.fast_charge_ratio,
  a.avg_wait_duration_min,

  (SELECT json_arr FROM station_rank_abn)     AS top_abnormal_stations,
  (SELECT json_arr FROM station_rank_quality) AS top_quality_stations,

  (a.total_charge_session_cnt - p.prev_total_charge_session_cnt)/NULLIF(p.prev_total_charge_session_cnt,0)*100
                                             AS wow_charge_cnt_change_rate,
  (a.abnormal_rate - p.prev_abnormal_rate)    AS wow_abnormal_rate_change
FROM agg a
CROSS JOIN prev p;