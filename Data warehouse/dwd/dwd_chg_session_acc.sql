DROP TABLE IF EXISTS dwd_chg_session_acc;

CREATE TABLE IF NOT EXISTS dwd_chg_session_acc (
    -- 主键与维度
    session_id                  STRING      COMMENT '充电会话ID（主键）',
    vin                         STRING      COMMENT 'VIN码',
    vehicle_sk                  BIGINT      COMMENT '车辆代理键',
    user_id                     STRING      COMMENT '用户ID',
    charger_id                  STRING      COMMENT '充电桩ID（退化维度）',
    pile_id                     STRING      COMMENT '充电枪ID',
    station_id                  STRING      COMMENT '充电站ID',
    station_name                STRING      COMMENT '充电站名称',
    
    -- 地理位置
    lat                         DOUBLE      COMMENT '充电站纬度',
    lon                         DOUBLE      COMMENT '充电站经度',
    province                    STRING      COMMENT '省份',
    city                        STRING      COMMENT '城市',
    
    -- 时间里程碑（累积快照核心字段）
    plug_in_time                TIMESTAMP   COMMENT '插枪时间',
    start_charge_time           TIMESTAMP   COMMENT '开始充电时间',
    end_charge_time             TIMESTAMP   COMMENT '结束充电时间',
    pull_out_time               TIMESTAMP   COMMENT '拔枪时间',
    payment_time                TIMESTAMP   COMMENT '支付时间',
    
    -- 时效度量（派生字段）
    wait_duration_min           INT         COMMENT '等待时长（分钟）：start - plug_in',
    charge_duration_min         INT         COMMENT '充电时长（分钟）：end - start',
    total_duration_min          INT         COMMENT '总时长（分钟）：pull_out - plug_in',
    
    -- 累积度量
    start_soc                   DOUBLE      COMMENT '开始SOC（%）',
    end_soc                     DOUBLE      COMMENT '结束SOC（%）',
    soc_gain                    DOUBLE      COMMENT 'SOC增量（%）',
    charged_energy              DOUBLE      COMMENT '充电电量（kWh）',
    charged_amount              DOUBLE      COMMENT '充电金额（元）',
    
    -- 充电特征
    avg_power                   DOUBLE      COMMENT '平均功率（kW）',
    max_power                   DOUBLE      COMMENT '最大功率（kW）',
    max_temp_during_charge      DOUBLE      COMMENT '充电期间最高温度（℃）',
    
    -- 状态字段
    session_status              STRING      COMMENT '会话状态：CHARGING/COMPLETED/ABNORMAL/PAID',
    stop_reason                 STRING      COMMENT '停止原因：USER/FULL/TIMEOUT/FAULT',
    
    -- 异常标识
    is_abnormal_stop            TINYINT     COMMENT '异常中断标识：1-异常 0-正常',
    
    -- ETL元数据
    etl_date                    STRING      COMMENT 'ETL处理日期',
    create_time                 TIMESTAMP   COMMENT '记录创建时间',
    update_time                 TIMESTAMP   COMMENT '记录更新时间'
)
COMMENT '充电行程累积事实表（累积快照事实表）'
PARTITIONED BY (dt STRING COMMENT '分区字段：插枪日期 yyyy-MM-dd')
STORED AS ORC
TBLPROPERTIES (
    'orc.compress'='SNAPPY'
);

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

-- 临时表：当日ODS新增/更新的会话
DROP TABLE IF EXISTS tmp_chg_session_new;
CREATE TEMPORARY TABLE tmp_chg_session_new AS
SELECT
    -- 主键与维度
    t.session_id,
    t.vin,
    COALESCE(v.vehicle_sk, -1) AS vehicle_sk,
    t.user_id,
    t.pile_id AS charger_id,
    t.pile_id,
    t.station_id,
    s.station_name,
    
    -- 位置
    s.lat,
    s.lon,
    s.province,
    s.city,
    
    -- 时间里程碑
    t.plug_in_time,
    t.start_charge_time,
    t.end_charge_time,
    t.pull_out_time,
    NULL AS payment_time,  -- 如ODS无此字段，后续从支付表关联
    
    -- 时效度量
    CAST((UNIX_TIMESTAMP(t.start_charge_time) - UNIX_TIMESTAMP(t.plug_in_time)) / 60 AS INT) AS wait_duration_min,
    CAST((UNIX_TIMESTAMP(t.end_charge_time) - UNIX_TIMESTAMP(t.start_charge_time)) / 60 AS INT) AS charge_duration_min,
    CAST((UNIX_TIMESTAMP(t.pull_out_time) - UNIX_TIMESTAMP(t.plug_in_time)) / 60 AS INT) AS total_duration_min,
    
    -- 累积度量
    t.start_soc / 10.0 AS start_soc,
    t.end_soc / 10.0 AS end_soc,
    (t.end_soc - t.start_soc) / 10.0 AS soc_gain,
    t.charged_energy / 100.0 AS charged_energy,  -- 假设单位0.01kWh
    t.charged_amount / 100.0 AS charged_amount,  -- 分 -> 元
    
    -- 充电特征
    CASE 
        WHEN (UNIX_TIMESTAMP(t.end_charge_time) - UNIX_TIMESTAMP(t.start_charge_time)) > 0 
        THEN (t.charged_energy / 100.0) / ((UNIX_TIMESTAMP(t.end_charge_time) - UNIX_TIMESTAMP(t.start_charge_time)) / 3600.0)
        ELSE NULL 
    END AS avg_power,
    NULL AS max_power,  -- 需从监控明细表聚合获取
    NULL AS max_temp_during_charge,
    
    -- 状态
    CASE t.session_status
        WHEN 1 THEN 'CHARGING'
        WHEN 2 THEN 'COMPLETED'
        WHEN 3 THEN 'ABNORMAL'
        WHEN 4 THEN 'PAID'
        ELSE 'UNKNOWN'
    END AS session_status,
    t.stop_reason,
    
    -- 异常标识
    CASE WHEN t.session_status = 3 THEN 1 ELSE 0 END AS is_abnormal_stop,
    
    -- ETL元数据
    '${hiveconf:etl_date}' AS etl_date,
    CURRENT_TIMESTAMP() AS create_time,
    CURRENT_TIMESTAMP() AS update_time,
    
    -- 分区字段
    FROM_UNIXTIME(UNIX_TIMESTAMP(t.plug_in_time), 'yyyy-MM-dd') AS dt
    
FROM 
    ods_log_charging_session_inc t
LEFT JOIN 
    dim_vehicle_zip v 
    ON t.vin = v.vin 
    AND FROM_UNIXTIME(UNIX_TIMESTAMP(t.plug_in_time), 'yyyy-MM-dd') BETWEEN v.start_date AND v.end_date
LEFT JOIN
    ods_biz_charging_station_full s
    ON t.station_id = s.station_id
    AND s.dt = '${hiveconf:etl_date}'
WHERE 
    t.dt = '${hiveconf:etl_date}';

-- 合并逻辑：历史 + 新增，按session_id去重保留最新
INSERT OVERWRITE TABLE dwd_chg_session_acc PARTITION(dt)
SELECT
    session_id, vin, vehicle_sk, user_id, charger_id, pile_id, station_id, station_name,
    lat, lon, province, city,
    plug_in_time, start_charge_time, end_charge_time, pull_out_time, payment_time,
    wait_duration_min, charge_duration_min, total_duration_min,
    start_soc, end_soc, soc_gain, charged_energy, charged_amount,
    avg_power, max_power, max_temp_during_charge,
    session_status, stop_reason, is_abnormal_stop,
    etl_date, create_time, update_time,
    dt
FROM (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY update_time DESC) AS rn
    FROM (
        -- 历史分区数据（近7天）
        SELECT * FROM dwd_chg_session_acc 
        WHERE dt >= DATE_SUB('${hiveconf:etl_date}', 7)
        
        UNION ALL
        
        -- 当日新增/更新
        SELECT * FROM tmp_chg_session_new
    ) t
) t2
WHERE rn = 1;