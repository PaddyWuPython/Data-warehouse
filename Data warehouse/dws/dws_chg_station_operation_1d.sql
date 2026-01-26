CREATE TABLE IF NOT EXISTS dws_chg_station_operation_1d (
    station_id                  STRING      COMMENT '充电站ID',
    station_name                STRING      COMMENT '充电站名称',
    province                    STRING      COMMENT '省份',
    city                        STRING      COMMENT '城市',
    stat_date                   STRING      COMMENT '统计日期',
    service_vehicle_cnt         INT         COMMENT '服务车辆数',
    total_session_cnt           INT         COMMENT '总充电次数',
    completed_session_cnt       INT         COMMENT '完成次数',
    abnormal_session_cnt        INT         COMMENT '异常次数',
    total_charged_energy        DOUBLE      COMMENT '总充电电量（kWh）',
    total_revenue               DOUBLE      COMMENT '总收入（元）',
    avg_revenue_per_session     DOUBLE      COMMENT '单次平均收入',
    pile_count                  INT         COMMENT '充电桩数量',
    total_charge_duration_hour  DOUBLE      COMMENT '总充电时长（小时）',
    avg_pile_utilization        DOUBLE      COMMENT '平均桩利用率（%）',
    peak_hour_utilization       DOUBLE      COMMENT '高峰时段利用率（%）',
    abnormal_rate               DOUBLE      COMMENT '异常率（%）',
    fault_pile_cnt              INT         COMMENT '故障桩数量',
    avg_charging_power          DOUBLE      COMMENT '平均充电功率（kW）'
)
COMMENT '充电站运营统计日表（DWS层）'
PARTITIONED BY (dt STRING)
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY');

