# DWD 层设计说明文档（Design Overview）

## 1. DWD 层设计概述（Design Overview）

DWD（Data Warehouse Detail）层是数仓中最核心、数据量最大的一层。它紧紧围绕业务过程（Business Process），对 ODS 层的原始数据进行清洗、规范化、维度退化（降维），并以事实表（Fact Table）的形式存储，为下游 DWS/ADS 的汇总分析提供稳定、统一、可复用的明细数据基础。

### 1.1 设计原则

**基于业务过程**：每个事实表对应一个具体的业务动作，例如“车辆行驶上报”“触发报警”“创建维修工单”等，避免以数据源表为中心的“照搬式建模”。

**原子粒度（Atomic Grain）**：保持最低粒度以保证数据细节不丢失，例如“每一次 10–30 秒上报”“每一个故障码”“每一次电压/温度采样”。在最低粒度上构建，才能支持未来任意维度的上卷、切片、钻取与回溯分析。

**维度退化（Degenerate Dimension / 冗余）**：为提升查询性能，将常用维度属性（如车型、电池类型、区域）从 DIM 层退化到事实表中，减少下游 Join，并为高频查询提供更直接的过滤条件。

**数据治理前置**：在 DWD 层完成核心的数据清洗与结构化处理，例如剔除 GPS 漂移点、统一物理单位（电压、电流等）、解析 JSON/ARRAY 结构、空值填充与异常值处理等，确保数据口径在数仓内部一致。

### 1.2 技术规范

**存储格式**：ORC（列式存储，加速查询） + SNAPPY（压缩，节省空间）  
**命名规范**：`dwd_{数据域}_{业务过程}_{表类型}`  

其中：
- 数据域：`drive`（行车），`batt`（电池），`chg`（充电），`safe`（安全/报警），`user`（用户）
- 表类型标识：  
  - `_inc`：事务型事实表（Transaction Fact），记录原子事件，增量加载  
  - `_acc`：累积快照事实表（Accumulating Snapshot Fact），记录流程全生命周期，需合并更新  
  - `_full`：周期快照事实表（Periodic Snapshot Fact），记录固定时间点状态，全量/增量快照  

---

## 2. 业务总线矩阵（Business Bus Matrix）

基于质量安全部门的五大业务域，构建总线矩阵以确立事实表与公共维度的一致性与复用关系。

| 业务域 | 业务过程（事实表） | 事实表类型 | 时间维 | 车辆维 | 电池维 | 故障维 | 地理维 | 用户维 | 描述 |
|---|---|---|---|---|---|---|---|---|---|
| 行车安全 | 车辆行驶日志 | 事务型 | √ | √ | √ |  | √ |  | 10–30 秒/条的高频工况 |
| 行车安全 | 驾驶行为事件 | 事务型 | √ | √ |  |  | √ | √ | 急刹、急转、超速等风险事件 |
| 电池安全 | 电池单体监测 | 事务型 | √ | √ | √ |  |  |  | 电芯级电压/温度监控 |
| 电池安全 | 电池极值监测 | 事务型 | √ | √ | √ |  |  |  | 最高/最低电压、温度等极值 |
| 充电安全 | 充电实时监控 | 事务型 | √ | √ | √ |  | √ |  | 充电过程电压/电流等监控 |
| 充电安全 | 充电行程订单 | 累积快照 | √ | √ | √ |  | √ | √ | 充电开始→结束的全流程 |
| 告警响应 | 车辆故障报警 | 事务型 | √ | √ | √ | √ | √ |  | 触发的具体报警信号/故障码 |
| 告警响应 | 报警处置工单 | 累积快照 | √ | √ |  | √ | √ | √ | 报警→派单→救援→关闭 |
| 用户服务 | 安全投诉记录 | 累积快照 | √ | √ |  |  | √ | √ | 投诉→处理→回访 |

---

## 3. 详细模型设计（Detailed Model Design）

### 3.1 行车安全域（Driving Safety Domain）

#### 3.1.1 车辆行驶实时明细表（`dwd_drive_running_log_inc`）

**类型**：事务型事实表（Transaction Fact）  
**粒度**：每次 T-BOX 上报（约 10–30 秒/条）  
**设计要点**：该表是数仓中数据量最大的表，需从 ODS 层 `ods_log_vehicle_track_inc` 解析整车与电机等结构化数据，并保证口径统一与查询高效。

**核心字段（示例）**  
- 维度/退化字段：`vin`，`vehicle_sk`（退化），`model_name`（退化），`province_code`（退化）  
- 度量指标：`speed`（车速），`odometer`（累计里程），`total_voltage`，`total_current`，`soc`，`insulation_res`（绝缘电阻）  
- 状态标识：`vehicle_status`（启动/熄火），`run_mode`（纯电/混动），`gear_position`（挡位）  

**特殊处理（电机数组）**  
ODS 中 `motors` 为数组结构。若炸裂（`LATERAL VIEW EXPLODE`）会导致数据量膨胀 N 倍，建议在 DWD 层保留 ARRAY 结构（用于回放与明细查询）或仅提取“主电机”字段用于统计，避免大规模膨胀。

---

#### 3.1.2 驾驶行为风险事件表（`dwd_drive_behavior_event_inc`）

**类型**：事务型事实表  
**粒度**：每个风险事件一行  
**数据来源**：T-BOX 边缘计算上报，或实时计算引擎识别后落库  

**核心字段（示例）**  
- 维度字段：`vin`，`user_id`（驾驶员），`location_geo`  
- 属性字段：`event_type`（急加速/急刹/急转/未系安全带等），`risk_level`（高/中/低）  
- 度量字段：`duration_sec`，`start_speed`，`end_speed`，`acceleration_g`（加速度 G 值）  

---

### 3.2 电池安全域（Battery Safety Domain）

#### 3.2.1 电池单体监测明细表（`dwd_batt_cell_log_inc`）

**类型**：事务型事实表  
**粒度**：每次上报  

**设计难点**：单车可能包含 100+ 单体电压/温度，如将单体炸裂为多行，数据量会达到整车表的 100 倍，极易走向 PB 级。

**存储策略**：在 DWD 层保留 ARRAY 结构，不炸裂。Hive 可直接对 ARRAY 操作（如 `sort_array`、`array_contains`），便于在不膨胀数据量的情况下完成筛查与计算。

**核心字段（示例）**  
- `cell_voltages: ARRAY<DOUBLE>`  
- `cell_temps: ARRAY<DOUBLE>`  

**派生指标（ETL 计算后入库）**  
- `max_cell_voltage`：最高单体电压  
- `min_cell_voltage`：最低单体电压  
- `voltage_diff`：压差（`max - min`），电池一致性核心指标  
- `avg_temp`：平均温度  

---

#### 3.2.2 电池极值与异常表（`dwd_batt_extreme_log_inc`）

**类型**：事务型事实表  
**粒度**：每次上报  

**描述**：记录 BMS 计算输出的极值信息，用于快速筛选热失控风险车辆与异常电芯定位。

**核心字段（示例）**  
- 最高电压：`max_voltage_system_no`，`max_voltage_cell_no`，`max_voltage_value`  
- 最低电压：`min_voltage_system_no`，`min_voltage_cell_no`，`min_voltage_value`  
- 最高温度：`max_temp_probe_no`，`max_temp_value`  

---

### 3.3 充电安全域（Charging Safety Domain）

#### 3.3.1 充电过程监控明细表（`dwd_chg_monitor_log_inc`）

**类型**：事务型事实表  
**粒度**：充电过程中的每次上报（通常频率高于行驶数据）  

**核心字段（示例）**  
- 维度标识：`charger_id`（充电桩 ID，关联 DIM），`station_id`  
- 过程度量：`charging_voltage`，`charging_current`  
- 状态字段：`bms_charge_status`（BMS 请求状态）  
- 风险监控：`cell_max_temp_during_charge`（充电最高电芯温度，用于监控析锂/过热风险）  

---

#### 3.3.2 充电行程累积事实表（`dwd_chg_session_acc`）

**类型**：累积快照事实表（Accumulating Snapshot Fact）  
**粒度**：一次完整的充电会话（Session）  
**设计要点**：充电进行中会不断更新同一行数据的里程碑时间与累计度量，直到充电结束。

**核心字段（示例）**  
- 标识：`session_id`，`vin`，`pile_id`  
- 时间里程碑：`plug_in_time`，`start_charge_time`，`end_charge_time`，`payment_time`  
- 累积度量：`charged_energy`（kWh），`duration_min`，`start_soc`，`end_soc`  
- 状态：`session_status`（充电中/已结束/异常中断/支付完成）  

---

### 3.4 告警响应域（Alarm Response Domain）

#### 3.4.1 车辆故障报警明细表（`dwd_safe_alarm_detail_inc`）

**类型**：事务型事实表  
**粒度**：每个被触发的故障码（Fault Code）一行  

**ETL 逻辑**：将 ODS 中 `fault_codes` 数组炸裂（`EXPLODE`），实现“一条报警记录 → 多条故障码明细”。

**核心字段（示例）**  
- 事实字段：`vin`，`alarm_time`，`fault_hex_code`（原始 Hex 码），`fault_level`（1/2/3 级）  
- 维度退化字段（来自故障维表）：`fault_name`（故障名称），`solution_guide`（维修建议）  
- 业务标识：`is_safety_related`（是否安全相关；如绝缘故障为是，多媒体故障为否）  

---

#### 3.4.2 报警处置工单累积表（`dwd_safe_alarm_workorder_acc`）

**类型**：累积快照事实表  
**粒度**：一个处置工单（严重报警触发生成的任务）  

**核心字段（示例）**  
- 标识：`workorder_id`，`vin`，`alarm_trigger_time`  
- 里程碑时间：`create_time`，`assign_time`，`contact_user_time`，`rescue_arrive_time`，`close_time`  
- 时效度量：`response_lag_sec`（响应耗时），`handle_duration_hour`（处理耗时）  
- 结果字段：`handle_result`（误报/远程指导/现场救援/拖车），`feedback_score`  

---

### 3.5 用户服务域（User Service Domain）

#### 3.5.1 用户安全投诉累积表（`dwd_user_complaint_acc`）

**类型**：累积快照事实表  
**粒度**：一个投诉单  

**核心字段（示例）**  
- 标识：`complaint_id`，`user_id`，`vin`  
- 属性：`complaint_type`（刹车失灵/自燃/异响等）  
- 里程碑：`submit_time`，`accept_time`，`verify_time`，`close_time`  
- 结果：`is_quality_issue`（是否质量问题，0/1）  

---

## 4. 构建说明与优化策略

### 4.1 数据一致性保证

**SK 关联原则**：所有事实表中的 `vehicle_sk` 必须通过 `vin + collect_time` 关联 `dim_vehicle_zip` 获取，确保取到“事发当时”的车辆状态（例如当时归属地、当时车主、当时车型配置），避免用最新状态覆盖历史事实。

**统一单位与口径**：ODS 层为了传输效率常以整数存储物理量（例如电压以 0.1V 量纲存为 Int）。DWD 层必须按比例因子还原为 Double 类型的真实物理值（例如 3505 → 350.5V），并保证跨域指标口径一致。

### 4.2 存储与压缩优化

**ORC 列式存储**：DWD 表字段常达 100+ 列，但分析通常只选少量列（如 SOC、温度）。ORC 配合 Hive 矢量化执行可显著降低 I/O，并提升大表扫描性能。

**Snappy 压缩**：在压缩比与 CPU 解压成本之间取得平衡，适合流式写入与高频读取场景。

**排序（Sorting）建议**：日志类大表建议建表时按 `SORT BY (vin, collect_time)`，使 ORC 文件内部更有序，增强谓词下推与过滤效率，显著加速 `WHERE vin = '...'` 这类查询。

### 4.3 累积快照表（`_acc`）的实现策略

由于 Hive 不支持高效行级更新，`_acc` 表通常采用“分区重写”或“合并更新”的方式实现状态滚动。

**分区策略**：一般按业务创建日期（如 `create_date`）分区，而非 ETL 处理日期，确保同一业务单据的生命周期落在稳定分区内便于合并。

**更新逻辑**：每日读取 ODS 当日增量 + DWD 历史分区数据，按主键（如 `session_id` / `workorder_id`）进行合并（Merge），然后重写对应分区。这样能够保证流程状态（如 Open→Closed）在同一行中持续更新，并同步更新里程碑时间字段与累计指标。
