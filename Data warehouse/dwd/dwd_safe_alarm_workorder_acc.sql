DROP TABLE IF EXISTS dwd_safe_alarm_workorder_acc;

CREATE TABLE IF NOT EXISTS dwd_safe_alarm_workorder_acc (
    -- 主键与维度
    workorder_id                STRING      COMMENT '工单ID（主键）',
    vin                         STRING      COMMENT 'VIN码',
    vehicle_sk                  BIGINT      COMMENT '车辆代理键',
    alarm_record_id             STRING      COMMENT '关联报警记录ID',
    user_id                     STRING      COMMENT '用户ID',
    
    -- 时间里程碑
    alarm_trigger_time          TIMESTAMP   COMMENT '报警触发时间',
    create_time_wo              TIMESTAMP   COMMENT '工单创建时间',
    assign_time                 TIMESTAMP   COMMENT '派单时间',
    contact_user_time           TIMESTAMP   COMMENT '联系用户时间',
    rescue_arrive_time          TIMESTAMP   COMMENT '救援到达时间',
    close_time                  TIMESTAMP   COMMENT '工单关闭时间',
    
    -- 时效度量
    response_lag_sec            INT         COMMENT '响应耗时（秒）：create - trigger',
    handle_duration_hour        DOUBLE      COMMENT '处理耗时（小时）：close - create',
    
    -- 工单属性
    workorder_status            STRING      COMMENT '工单状态：OPEN/ASSIGNED/IN_PROGRESS/CLOSED',
    priority                    STRING      COMMENT '优先级：HIGH/MEDIUM/LOW',
    handle_result               STRING      COMMENT '处理结果：误报/远程指导/现场救援/拖车',
    
    -- 结果评价
    feedback_score              INT         COMMENT '用户反馈评分（1-5分）',
    is_resolved                 TINYINT     COMMENT '是否解决：1-是 0-否',
    
    -- 操作人员
    operator_id                 STRING      COMMENT '处理人员ID',
    rescue_team_id              STRING      COMMENT '救援队伍ID',
    
    -- 位置
    lat                         DOUBLE      COMMENT '报警位置纬度',
    lon                         DOUBLE      COMMENT '报警位置经度',
    
    -- ETL元数据
    etl_date                    STRING      COMMENT 'ETL处理日期',
    create_time_etl             TIMESTAMP   COMMENT 'ETL记录创建时间',
    update_time                 TIMESTAMP   COMMENT '记录更新时间'
)
COMMENT '报警处置工单累积表（累积快照事实表）'
PARTITIONED BY (dt STRING COMMENT '分区字段：工单创建日期 yyyy-MM-dd')
STORED AS ORC
TBLPROPERTIES (
    'orc.compress'='SNAPPY'
);

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

-- 临时表：解析当日Maxwell增量
DROP TABLE IF EXISTS tmp_alarm_workorder_new;
CREATE TEMPORARY TABLE tmp_alarm_workorder_new AS
SELECT
    -- 解析JSON的data字段
    get_json_object(data, '$.workorder_id') AS workorder_id,
    get_json_object(data, '$.vin') AS vin,
    get_json_object(data, '$.alarm_record_id') AS alarm_record_id,
    get_json_object(data, '$.user_id') AS user_id,
    
    -- 时间里程碑
    FROM_UNIXTIME(get_json_object(data, '$.alarm_trigger_time')) AS alarm_trigger_time,
    FROM_UNIXTIME(get_json_object(data, '$.create_time')) AS create_time_wo,
    FROM_UNIXTIME(get_json_object(data, '$.assign_time')) AS assign_time,
    FROM_UNIXTIME(get_json_object(data, '$.contact_user_time')) AS contact_user_time,
    FROM_UNIXTIME(get_json_object(data, '$.rescue_arrive_time')) AS rescue_arrive_time,
    FROM_UNIXTIME(get_json_object(data, '$.close_time')) AS close_time,
    
    -- 属性
    get_json_object(data, '$.status') AS workorder_status,
    get_json_object(data, '$.priority') AS priority,
    get_json_object(data, '$.handle_result') AS handle_result,
    CAST(get_json_object(data, '$.feedback_score') AS INT) AS feedback_score,
    CAST(get_json_object(data, '$.is_resolved') AS TINYINT) AS is_resolved,
    
    -- 人员
    get_json_object(data, '$.operator_id') AS operator_id,
    get_json_object(data, '$.rescue_team_id') AS rescue_team_id,
    
    -- 位置
    CAST(get_json_object(data, '$.lat') AS DOUBLE) AS lat,
    CAST(get_json_object(data, '$.lon') AS DOUBLE) AS lon,
    
    -- 分区
    FROM_UNIXTIME(get_json_object(data, '$.create_time'), 'yyyy-MM-dd') AS dt,
    
    -- 更新时间（Maxwell的ts字段）
    FROM_UNIXTIME(CAST(ts AS BIGINT) / 1000) AS update_time
    
FROM 
    ods_biz_alarm_handling_inc
WHERE 
    dt = '${hiveconf:etl_date}'
    AND type IN ('insert', 'update');  -- Maxwell事件类型

-- 合并逻辑
INSERT OVERWRITE TABLE dwd_safe_alarm_workorder_acc PARTITION(dt)
SELECT
    t.workorder_id,
    t.vin,
    COALESCE(v.vehicle_sk, -1) AS vehicle_sk,
    t.alarm_record_id,
    t.user_id,
    
    -- 时间里程碑
    t.alarm_trigger_time,
    t.create_time_wo,
    t.assign_time,
    t.contact_user_time,
    t.rescue_arrive_time,
    t.close_time,
    
    -- 时效度量
    CAST(UNIX_TIMESTAMP(t.create_time_wo) - UNIX_TIMESTAMP(t.alarm_trigger_time) AS INT) AS response_lag_sec,
    CAST((UNIX_TIMESTAMP(t.close_time) - UNIX_TIMESTAMP(t.create_time_wo)) / 3600.0 AS DOUBLE) AS handle_duration_hour,
    
    -- 属性
    t.workorder_status,
    t.priority,
    t.handle_result,
    t.feedback_score,
    t.is_resolved,
    
    -- 人员
    t.operator_id,
    t.rescue_team_id,
    
    -- 位置
    t.lat,
    t.lon,
    
    -- ETL元数据
    '${hiveconf:etl_date}' AS etl_date,
    CURRENT_TIMESTAMP() AS create_time_etl,
    t.update_time,
    
    t.dt
    
FROM (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY workorder_id ORDER BY update_time DESC) AS rn
    FROM (
        -- 历史分区
        SELECT * FROM dwd_safe_alarm_workorder_acc 
        WHERE dt >= DATE_SUB('${hiveconf:etl_date}', 30)  -- 保留30天窗口
        
        UNION ALL
        
        -- 新增/更新
        SELECT 
            workorder_id, vin, -1 AS vehicle_sk, alarm_record_id, user_id,
            alarm_trigger_time, create_time_wo, assign_time, contact_user_time, rescue_arrive_time, close_time,
            NULL AS response_lag_sec, NULL AS handle_duration_hour,
            workorder_status, priority, handle_result, feedback_score, is_resolved,
            operator_id, rescue_team_id, lat, lon,
            '${hiveconf:etl_date}' AS etl_date, CURRENT_TIMESTAMP() AS create_time_etl, update_time,
            dt
        FROM tmp_alarm_workorder_new
    ) all_data
) t
LEFT JOIN 
    dim_vehicle_zip v 
    ON t.vin = v.vin 
    AND t.dt BETWEEN v.start_date AND v.end_date
WHERE rn = 1;