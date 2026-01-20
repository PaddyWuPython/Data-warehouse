# ODS层构建说明书
---

## 1. 设计概述 (Design Overview)

ODS（Operational Data Store）层作为数据仓库的贴源层，主要职责是**原样接入**上游车联网终端日志与业务系统数据，起到“数据缓冲”与“溯源备份”的作用。

### 1.1 设计原则

1. **保持原貌**：字段名、数据类型尽量与源系统保持一致，不进行深度的清洗与转换。
2. **分区管理**：所有表均需设置分区（Partition），通常按日期（`dt`）或日期+小时（`dt`, `hr`）分区，以支持增量处理与历史回溯。
3. **存储压缩**：针对海量数据采用压缩格式（Snappy/Gzip），平衡存储成本与I/O效率。
4. **宽表化处理（日志域）**：针对 GB/T 32960 协议的高频多报文特性，在 ODS 层进行适度的“宽表化”合并，避免产生大量小文件。

### 1.2 命名规范

表名格式：`ods_{来源}_{业务域}_{表名}_{同步方式}`

* `来源`：`log` (车联网日志), `biz` (业务数据库)
* `同步方式`：`inc` (增量/流水), `full` (全量快照)

---

## 2. 日志类数据表 (Log Tables)

**数据来源**：T-BOX 终端 -> 网关 -> Flume -> Kafka -> HDFS
**存储格式**：`TextFile` (JSON) 或 `ORC`
**压缩方式**：`Gzip` (TextFile) 或 `Snappy` (ORC)
**分区策略**：`dt` (日期: yyyy-MM-dd), `hr` (小时: 00-23)

### 2.1 核心设计：车辆实时轨迹宽表

为解决 GB/T 32960 协议中整车、电机、电池等多包频发导致的小文件问题，将相关流数据合并为一张宽表。

| 表名 | **`ods_log_vehicle_track_inc`** |
| --- | --- |
| **描述** | 车辆实时综合轨迹日志表，涵盖行驶、三电状态、报警等核心数据。 |
| **主要字段** | **Common**: `vin`, `collect_time` <br>

<br>**Vehicle**: `speed`, `odometer`, `status`, `soc`, `total_voltage`<br>

<br>**Motor**: `ARRAY<STRUCT>` (电机转速/转矩/温度)<br>

<br>**BMS**: `pack_voltage`, `pack_current`, `cell_volt_list` (单体电压数组), `probe_temp_list` (探针温度数组)<br>

<br>**Alarm**: `alarm_level`, `fault_codes` (故障码列表)<br>

<br>**Location**: `lat`, `lon`, `gps_status` |
| **用途** | 行车安全分析（超速/故障）、电池安全分析（压差/热失控）、轨迹回放。 |

### 2.2 事件与流水表

| 表名 | 描述 | 关键字段 | 用途 |
| --- | --- | --- | --- |
| **`ods_log_vehicle_sys_inc`** | **系统事件流水表**<br>

<br>合并登入、登出、终端异常。 | `vin`, `event_type` (LOGIN/LOGOUT/ERROR), `iccid`, `duration` | 计算车辆在线率、T-BOX 稳定性监控。 |
| **`ods_log_safety_risk_inc`** | **安全预警事件表**<br>

<br>记录急刹、急转、疲劳驾驶等瞬时事件。 | `vin`, `risk_type` (HARD_BRAKE/FATIGUE), `g_value`, `start_speed`, `end_speed` | 驾驶行为评分、用户安全画像。 |
| **`ods_log_charging_session_inc`** | **充电行程结算表**<br>

<br>记录一次完整充电的起止状态。 | `vin`, `session_id`, `station_id`, `charged_energy`, `start_soc`, `end_soc` | 充电桩利用率分析、充电异常中断分析。 |
| **`ods_log_app_feedback_inc`** | **用户APP反馈表**<br>

<br>用户主动上传的日志/图片。 | `user_id`, `vin`, `content`, `media_urls`, `feedback_type` | 舆情监控、故障辅助诊断。 |

---

## 3. 业务类数据表 (Business Tables)

### 3.1 全量同步表 (Full Snapshot)

**数据来源**：MySQL 业务库 -> DataX -> Hive
**存储格式**：`ORC`
**分区策略**：`dt` (每日一分区，存储当日全量快照)

| 业务域 | 表名 | 描述 | 关键字段 |
| --- | --- | --- | --- |
| **车辆档案** | **`ods_biz_vehicle_master_full`** | 车辆主档案 | `vin`, `plate_no`, `sales_date`, `region_code` |
|  | **`ods_biz_vehicle_model_full`** | 车型配置 | `model_id`, `battery_type`, `nedc_range` |
| **用户信息** | **`ods_biz_user_master_full`** | 用户信息 | `user_id`, `gender`, `age`, `city` |
|  | **`ods_biz_vehicle_owner_rel_full`** | 人车关系 | `vin`, `user_id`, `relation_type` (车主/授权) |
| **零部件** | **`ods_biz_battery_pack_full`** | 电池包BOM | `pack_code`, `cell_model`, `cell_count`, `supplier_id` |
|  | **`ods_biz_motor_assembly_full`** | 电机总成 | `motor_code`, `peak_power`, `max_torque` |
|  | **`ods_biz_sim_card_full`** | SIM卡信息 | `iccid`, `operator`, `flow_limit` |
| **配置字典** | **`ods_biz_fault_code_dict_full`** | 故障码字典 | `dtc_code`, `description`, `risk_level` |
|  | **`ods_biz_alarm_rule_config_full`** | 报警规则 | `rule_id`, `threshold_value`, `metric_name` |
| **充电设施** | **`ods_biz_charging_station_full`** | 充电站主数据 | `station_id`, `lat`, `lon`, `pile_count` |
|  | **`ods_biz_charging_pile_full`** | 充电桩主数据 | `pile_id`, `power_rating`, `connector_type` |

### 3.2 增量同步表 (Incremental)

**数据来源**：MySQL Binlog -> Maxwell -> Kafka -> Flume -> Hive
**存储格式**：`TextFile` (JSON) - 保留 Maxwell 原始结构 (`type`, `ts`, `data`, `old`)
**分区策略**：`dt` (按产生时间分区)

| 业务域 | 表名 | 描述 | 关键字段 (在 data 结构体中) |
| --- | --- | --- | --- |
| **售后服务** | **`ods_biz_safety_complaint_inc`** | 安全投诉工单 | `complaint_id`, `complaint_type`, `status` |
|  | **`ods_biz_maintenance_order_inc`** | 维修保养工单 | `order_id`, `repair_parts_list`, `cost` |
| **事故报警** | **`ods_biz_accident_report_inc`** | 事故上报记录 | `accident_level`, `is_fire`, `weather`, `road_condition` |
|  | **`ods_biz_alarm_handling_inc`** | 报警处置记录 | `handle_result`, `response_time`, `operator_id` |

---

## 4. 技术实现关键点

### 4.1 复杂数据类型应用

在日志表中，大量使用了 Hive 的复杂数据类型以减少行数并保持数据内聚：

* **STRUCT**: 用于封装子模块数据，如 `vehicle` (车速/里程/SOC)、`location` (经纬度)。
* **ARRAY**: 用于存储列表型数据，如 `cells` (成百上千个单体电压)、`motors` (1-4个电机状态)。
* *优势*：避免了传统数仓中将 1 秒的数据拆成几百行（每个单体一行）导致的存储膨胀。



### 4.2 数据装载与解析

* **DataX (全量)**：直接将 MySQL 数据映射为 Hive ORC 文件的列，性能最高。
* **Maxwell (增量)**：ODS 层存储 JSON 字符串，DWD 层再通过 `get_json_object` 或 `json_tuple` 解析。这样做是为了处理 Schema Drift（上游表结构变更），防止 ETL 任务报错。

### 4.3 存储优化

* **ORC**: 选用 ORC 格式作为主要存储格式（特别是 DWD 层及全量 ODS 表），因为它支持 ACID 事务（部分场景），且列式存储对分析型查询（只查特定几列）非常友好。
* **Snappy**: 配合 ORC 使用 Snappy 压缩，解压速度快，适合高吞吐的离线计算场景。
