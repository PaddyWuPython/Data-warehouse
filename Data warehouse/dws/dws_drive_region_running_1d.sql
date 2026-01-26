CREATE TABLE IF NOT EXISTS dws_drive_region_running_1d (
    province_code               STRING      COMMENT '省份代码',
    city_code                   STRING      COMMENT '城市代码',
    province_name               STRING      COMMENT '省份名称',
    city_name                   STRING      COMMENT '城市名称',
    stat_date                   STRING      COMMENT '统计日期',
    active_vehicle_cnt          INT         COMMENT '活跃车辆数',
    online_vehicle_cnt          INT         COMMENT '在线车辆数',
    total_mileage               DOUBLE      COMMENT '区域总里程（km）',
    avg_mileage_per_vehicle     DOUBLE      COMMENT '车均里程',
    total_driving_duration_hour DOUBLE      COMMENT '总行驶时长（小时）',
    total_risk_event_cnt        INT         COMMENT '总风险事件数',
    risk_vehicle_cnt            INT         COMMENT '有风险事件的车辆数',
    avg_driving_score           DOUBLE      COMMENT '平均驾驶评分',
    accident_cnt                INT         COMMENT '事故次数',
    accident_vehicle_cnt        INT         COMMENT '事故车辆数'
)
COMMENT '区域行驶统计日表（DWS层）'
PARTITIONED BY (dt STRING)
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY');

