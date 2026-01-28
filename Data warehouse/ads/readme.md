## 4.1 ADS 层定位与设计要点

### 4.1.1 层定位
ADS 层是面向应用的数据服务层，直接对接 BI 报表、监控大屏、数据产品/API、外部 OLAP/关系型数据库。其核心目标是将 DWS 的指标结果“产品化交付”，做到**口径统一、可直接查询、无需二次加工**。

### 4.1.2 关键特性
- **应用导向**：一张 ADS 表对应一个明确的应用场景（日报/周报/月报/监控大屏/API）。
- **不分区（默认）**：ADS 表通常数据量较小（千～万级），以全表覆盖或增量追加为主，不强制设置分区字段。
- **行式存储（默认）**：采用 TextFile 或默认存储格式（非 ORC），便于与 MySQL/PostgreSQL/ClickHouse 等外部系统同步与落库。
- **宽表自包含**：表内包含完整的维度与指标字段，应用侧尽量不再 Join 其他表；复杂结构信息（TopN、分布、明细列表）以 JSON 字符串承载。
- **高时效性**：支持 T+1 日批处理，也可按业务需要扩展到小时级或 15 分钟级刷新。
- **可同步性**：天然适配 DataX/Sqoop/Spark 等同步工具，将 Hive 侧结果写入外部数据库供查询。

### 4.1.3 数据刷新与幂等策略（建议）
- **日报/周报/月报**：推荐“全表覆盖”，保证消费侧永远读取最新一期结果，减少重复数据与版本管理成本。
- **监控类**：推荐“快照模式”（只保留当前时点）或“快照 + 历史明细”双表模式；本方案默认采用快照表交付给大屏。
- **幂等性**：同一统计周期重复跑数结果应一致；如落外部库，建议以 `stat_date/stat_week/stat_month` 作为业务主键做 upsert 或 replace。

---

## 4.2 ADS 表清单与字段口径说明

本层面向五大业务域输出应用级宽表，覆盖：行车安全、电池安全、充电安全、告警响应、用户安全，并提供跨域综合视图与车型对标视图。

---

## 4.3 综合安全日报（`ads_safe_comprehensive_daily_report`）

### 4.3.1 业务场景
质量安全部门每日全域安全概览，用于 BI 日报、例会看板、风险汇总通报与经营分析。

### 4.3.2 刷新频率与同步
- **刷新频率**：每日 T+1（凌晨批处理）
- **同步目标**：MySQL（BI 报表系统/数据服务接口）

### 4.3.3 数据来源（口径追溯）
- 行车安全域：车辆行驶日汇总、驾驶行为日汇总（来自 DWS 日表）
- 电池安全域：电池健康日汇总、电池异常日汇总（来自 DWS 日表）
- 充电安全域：车辆充电日汇总（来自 DWS 日表）
- 告警响应域：车辆报警日汇总、告警处置效率日汇总（来自 DWS 日表）
- 用户安全域：用户投诉日汇总（来自 DWS 日表）

### 4.3.4 字段分组说明（不含 SQL）
- **时间维度**
  - `stat_date`：统计日期（日报日期）
  - `report_generate_time`：报表生成时间
- **行车安全域**
  - `driving_active_vehicle_cnt`：当日有有效行驶里程的车辆数
  - `driving_total_mileage`：当日全量里程汇总
  - `driving_total_risk_event_cnt`：当日风险事件总次数
  - `driving_risk_event_per_10k_km`：万公里风险事件率
  - `driving_avg_score`：当日驾驶评分均值
  - `driving_accident_cnt`：事故次数（如需对接事故数据源可扩展）
- **电池安全域**
  - `battery_monitored_vehicle_cnt`：当日进入电池监控/采样的车辆数
  - `battery_abnormal_vehicle_cnt`：当日出现电池异常（如异常持续时长>0）的车辆数
  - `battery_thermal_risk_cnt`：当日触发热风险判定的车辆数
  - `battery_avg_max_temp`：当日电池最高温度均值
  - `battery_avg_voltage_diff`：当日压差均值
  - `battery_over_temp_cnt`：当日过温次数汇总
  - `battery_avg_soh`：当日 SOH 均值
- **充电安全域**
  - `charging_total_session_cnt`：当日充电会话总数
  - `charging_abnormal_cnt`：当日异常中断会话数
  - `charging_abnormal_rate`：异常率
  - `charging_total_energy`：当日累计充电电量
  - `charging_over_temp_cnt`：当日充电过温次数
  - `charging_avg_power`：当日平均充电功率
- **告警响应域**
  - `alarm_total_cnt`：当日报警总次数
  - `alarm_vehicle_cnt`：当日触发报警的车辆数
  - `alarm_level_3_cnt`：当日严重报警次数
  - `alarm_safety_related_cnt`：当日安全相关报警次数
  - `alarm_avg_response_time_sec`：报警到响应平均时长（秒）
  - `alarm_response_sla_rate`：响应 SLA 达标率
  - `alarm_workorder_cnt`：工单数
  - `alarm_resolve_rate`：解决率
  - `alarm_avg_user_score`：用户评价均值
- **用户安全域**
  - `complaint_new_cnt`：当日新增投诉数
  - `complaint_safety_cnt`：当日安全类投诉数（可按业务字典口径定义）
  - `complaint_quality_confirm_cnt`：确认质量问题数
  - `complaint_avg_response_hour`：平均响应时长（小时）
  - `complaint_satisfaction_score`：满意度评分
  - `complaint_escalation_cnt`：升级投诉数
- **综合评估**
  - `overall_safety_score`：综合安全评分（0-100）
  - `risk_level`：综合风险等级（LOW/MEDIUM/HIGH）
  - `key_risk_items`：关键风险项摘要（JSON：如热风险车辆数、严重报警数、投诉升级数等）

---

## 4.4 电池安全实时监控（`ads_batt_safety_monitor_realtime`）

### 4.4.1 业务场景
监控大屏展示当前（小时级/15min）高风险车辆清单、关键风险指标与区域分布，支撑值班与应急处置。

### 4.4.2 刷新频率与同步
- **刷新频率**：小时级或 15 分钟级
- **同步目标**：Redis / ClickHouse（大屏/实时查询）

### 4.4.3 字段口径说明
- `stat_time`：统计时间点（快照时间）
- **总体概况**
  - `online_vehicle_cnt`：在线车辆数
  - `high_risk_vehicle_cnt`：高风险车辆数（满足热风险/压差风险/多指标综合阈值）
  - `alarm_vehicle_cnt`：报警车辆数
- **温度监控**
  - `over_60c_vehicle_cnt`：温度超过 60℃ 的车辆数
  - `max_temp_vin / max_temp_value / max_temp_location`：最高温车辆与位置摘要
- **压差监控**
  - `high_voltage_diff_cnt`：压差超过阈值（如 150mV）的车辆数
  - `max_voltage_diff_vin / max_voltage_diff_value`：最大压差车辆摘要
- **分布与列表（JSON）**
  - `risk_vehicle_by_province`：省份分布（JSON：省份->数量）
  - `top_risk_vehicles`：Top10 风险车辆列表（JSON 数组：vin、温度、压差、风险标签、位置等）
- **趋势对比**
  - `risk_vehicle_cnt_1h_ago`：一小时前高风险车辆数
  - `risk_vehicle_cnt_change`：风险车辆数变化量

---

## 4.5 充电质量周报（`ads_chg_quality_weekly_report`）

### 4.5.1 业务场景
周维度输出充电规模、异常质量、效率与站点排名，用于运营周报与充电网络质量复盘。

### 4.5.2 字段口径说明
- `stat_week`：统计周（yyyy-Www）
- `week_start_date / week_end_date`：周起止日期
- `report_generate_time`：报表生成时间
- **充电量指标**：总会话数、充电车辆数、总电量、单次平均电量、总充电时长
- **质量指标**：异常中断次数、异常率、过温会话数、过流会话数
- **效率指标**：平均功率、快充次数、快充占比、平均等待时长
- **站点排名（JSON）**
  - `top_abnormal_stations`：异常次数 Top10 站点
  - `top_quality_stations`：质量最优 Top10 站点（可按异常率/用户评分/成功率综合）
- **环比变化**：充电次数 WoW、异常率 WoW（百分点）

---

## 4.6 报警响应绩效月报（`ads_safe_alarm_response_monthly`）

### 4.6.1 业务场景
月维度输出报警与工单处置绩效，用于团队绩效考核、SLA 管控与故障治理分析。

### 4.6.2 字段口径说明
- `stat_month`：统计月份（yyyy-MM）
- `month_start_date / month_end_date`：月起止日期
- `report_generate_time`：报表生成时间
- **报警量指标**：报警次数、报警车辆数、报警车辆率、严重报警次数与占比
- **响应效率**：工单量、平均/中位响应时长、响应 SLA 达标率、平均处理时长、处理 SLA 达标率
- **处理质量**：解决率、误报率、用户评分与高分率
- **故障分析（JSON）**
  - `top_fault_codes`：高频故障码 Top10
  - `fault_category_distribution`：故障类别分布
  - `fault_recurrence_rate`：故障复发率
- **环比变化**：报警次数 MoM、响应 SLA MoM（百分点）

---

## 4.7 用户投诉分析月报（`ads_user_complaint_analysis_monthly`）

### 4.7.1 业务场景
月维度输出投诉规模、处理效率、质量确认、补偿与满意度，支持用户体验与质量改进闭环。

### 4.7.2 字段口径说明
- `stat_month`：统计月份
- `report_generate_time`：报表生成时间
- **投诉量**：总投诉数、安全类投诉数与占比、千车投诉率
- **投诉类型（JSON）**：Top5 投诉类型分布
- **处理时效**：平均响应时间、平均处理时长、响应/关闭 SLA 达标率
- **处理质量**：质量问题确认率、补偿率、平均补偿金额、满意度均值与达标率、升级率
- **区域与车型分布（JSON）**
  - `complaint_by_region`：区域分布 Top10
  - `complaint_by_model`：车型分布 Top10
- **环比变化**：投诉数 MoM、满意度 MoM（分值）

---

## 4.8 车型安全质量对比分析（`ads_model_safety_benchmark`）

### 4.8.1 业务场景
用于车型维度的安全质量横向对标（最近 30 天窗口），支撑产品质量评审、车型治理与管理层决策。

### 4.8.2 字段口径说明
- `model_name`：车型名称
- `stat_period`：统计周期标识（例如“最近30天”）
- `period_start_date / period_end_date`：窗口起止日期
- `vehicle_cnt`：纳入统计车辆数
- **行车安全**：车均里程、总里程、风险事件数、千公里风险事件率、平均驾驶评分
- **电池安全**：电池报警车辆数与报警率、平均压差、热风险车辆数与风险率、平均 SOH
- **充电安全**：充电次数、异常次数与异常率、平均充电温度
- **报警响应**：报警车辆数与报警率、严重报警次数、车均严重报警次数
- **用户投诉**：投诉数、千车投诉率、质量投诉数与占比
- **综合评分**：综合安全评分与安全排名（1=最优）

