DROP TABLE IF EXISTS dwd_user_complaint_acc;

CREATE TABLE IF NOT EXISTS dwd_user_complaint_acc (
    -- 主键与维度
    complaint_id                STRING      COMMENT '投诉单ID（主键）',
    user_id                     STRING      COMMENT '用户ID',
    vin                         STRING      COMMENT 'VIN码',
    vehicle_sk                  BIGINT      COMMENT '车辆代理键',
    
    -- 投诉属性
    complaint_type              STRING      COMMENT '投诉类型：刹车失灵/自燃/异响/电池衰减/其他',
    complaint_channel           STRING      COMMENT '投诉渠道：APP/400热线/经销商/监管平台',
    severity_level              STRING      COMMENT '严重程度：HIGH/MEDIUM/LOW',
    
    -- 时间里程碑
    submit_time                 TIMESTAMP   COMMENT '提交时间',
    accept_time                 TIMESTAMP   COMMENT '受理时间',
    verify_time                 TIMESTAMP   COMMENT '核实时间',
    resolve_time                TIMESTAMP   COMMENT '解决时间',
    close_time                  TIMESTAMP   COMMENT '关闭时间',
    callback_time               TIMESTAMP   COMMENT '回访时间',
    
    -- 时效度量
    response_lag_hour           DOUBLE      COMMENT '响应耗时（小时）',
    handle_duration_day         DOUBLE      COMMENT '处理耗时（天）',
    
    -- 处理结果
    complaint_status            STRING      COMMENT '投诉状态：SUBMITTED/ACCEPTED/VERIFIED/RESOLVED/CLOSED',
    is_quality_issue            TINYINT     COMMENT '是否质量问题：1-是 0-否',
    resolution_type             STRING      COMMENT '解决方式：退换车/维修/补偿/解释说明',
    compensation_amount         DOUBLE      COMMENT '补偿金额（元）',
    
    -- 评价
    user_satisfaction           INT         COMMENT '用户满意度（1-5分）',
    is_escalated                TINYINT     COMMENT '是否升级：1-是 0-否',
    
    -- 处理人员
    handler_id                  STRING      COMMENT '处理人员ID',
    department                  STRING      COMMENT '处理部门',
    
    -- 位置
    province                    STRING      COMMENT '省份',
    city                        STRING      COMMENT '城市',
    
    -- ETL元数据
    etl_date                    STRING      COMMENT 'ETL处理日期',
    create_time                 TIMESTAMP   COMMENT '记录创建时间',
    update_time                 TIMESTAMP   COMMENT '记录更新时间'
)
COMMENT '用户安全投诉累积表（累积快照事实表）'
PARTITIONED BY (dt STRING COMMENT '分区字段：投诉提交日期 yyyy-MM-dd')
STORED AS ORC
TBLPROPERTIES (
    'orc.compress'='SNAPPY'
);

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

-- 临时表：解析当日Maxwell增量
DROP TABLE IF EXISTS tmp_complaint_new;
CREATE TEMPORARY TABLE tmp_complaint_new AS
SELECT
    get_json_object(data, '$.complaint_id') AS complaint_id,
    get_json_object(data, '$.user_id') AS user_id,
    get_json_object(data, '$.vin') AS vin,
    
    -- 属性
    get_json_object(data, '$.complaint_type') AS complaint_type,
    get_json_object(data, '$.complaint_channel') AS complaint_channel,
    get_json_object(data, '$.severity_level') AS severity_level,
    
    -- 时间里程碑
    FROM_UNIXTIME(get_json_object(data, '$.submit_time')) AS submit_time,
    FROM_UNIXTIME(get_json_object(data, '$.accept_time')) AS accept_time,
    FROM_UNIXTIME(get_json_object(data, '$.verify_time')) AS verify_time,
    FROM_UNIXTIME(get_json_object(data, '$.resolve_time')) AS resolve_time,
    FROM_UNIXTIME(get_json_object(data, '$.close_time')) AS close_time,
    FROM_UNIXTIME(get_json_object(data, '$.callback_time')) AS callback_time,
    
    -- 结果
    get_json_object(data, '$.status') AS complaint_status,
    CAST(get_json_object(data, '$.is_quality_issue') AS TINYINT) AS is_quality_issue,
    get_json_object(data, '$.resolution_type') AS resolution_type,
    CAST(get_json_object(data, '$.compensation_amount') AS DOUBLE) / 100.0 AS compensation_amount,  -- 分->元
    
    -- 评价
    CAST(get_json_object(data, '$.user_satisfaction') AS INT) AS user_satisfaction,
    CAST(get_json_object(data, '$.is_escalated') AS TINYINT) AS is_escalated,
    
    -- 人员
    get_json_object(data, '$.handler_id') AS handler_id,
    get_json_object(data, '$.department') AS department,
    
    -- 位置
    get_json_object(data, '$.province') AS province,
    get_json_object(data, '$.city') AS city,
    
    -- 分区
    FROM_UNIXTIME(get_json_object(data, '$.submit_time'), 'yyyy-MM-dd') AS dt,
    
    -- 更新时间
    FROM_UNIXTIME(CAST(ts AS BIGINT) / 1000) AS update_time
    
FROM 
    ods_biz_safety_complaint_inc
WHERE 
    dt = '${hiveconf:etl_date}'
    AND type IN ('insert', 'update');

-- 合并逻辑
INSERT OVERWRITE TABLE dwd_user_complaint_acc PARTITION(dt)
SELECT
    t.complaint_id,
    t.user_id,
    t.vin,
    COALESCE(v.vehicle_sk, -1) AS vehicle_sk,
    
    -- 属性
    t.complaint_type,
    t.complaint_channel,
    t.severity_level,
    
    -- 时间里程碑
    t.submit_time,
    t.accept_time,
    t.verify_time,
    t.resolve_time,
    t.close_time,
    t.callback_time,
    
    -- 时效度量
    CAST((UNIX_TIMESTAMP(t.accept_time) - UNIX_TIMESTAMP(t.submit_time)) / 3600.0 AS DOUBLE) AS response_lag_hour,
    CAST((UNIX_TIMESTAMP(t.close_time) - UNIX_TIMESTAMP(t.submit_time)) / 86400.0 AS DOUBLE) AS handle_duration_day,
    
    -- 结果
    t.complaint_status,
    t.is_quality_issue,
    t.resolution_type,
    t.compensation_amount,
    
    -- 评价
    t.user_satisfaction,
    t.is_escalated,
    
    -- 人员
    t.handler_id,
    t.department,
    
    -- 位置
    t.province,
    t.city,
    
    -- ETL元数据
    '${hiveconf:etl_date}' AS etl_date,
    CURRENT_TIMESTAMP() AS create_time,
    t.update_time,
    
    t.dt
    
FROM (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY complaint_id ORDER BY update_time DESC) AS rn
    FROM (
        -- 历史分区
        SELECT * FROM dwd_user_complaint_acc 
        WHERE dt >= DATE_SUB('${hiveconf:etl_date}', 90)  -- 保留90天窗口
        
        UNION ALL
        
        -- 新增/更新
        SELECT 
            complaint_id, user_id, vin, -1 AS vehicle_sk,
            complaint_type, complaint_channel, severity_level,
            submit_time, accept_time, verify_time, resolve_time, close_time, callback_time,
            NULL AS response_lag_hour, NULL AS handle_duration_day,
            complaint_status, is_quality_issue, resolution_type, compensation_amount,
            user_satisfaction, is_escalated,
            handler_id, department, province, city,
            '${hiveconf:etl_date}' AS etl_date, CURRENT_TIMESTAMP() AS create_time, update_time,
            dt
        FROM tmp_complaint_new
    ) all_data
) t
LEFT JOIN 
    dim_vehicle_zip v 
    ON t.vin = v.vin 
    AND t.dt BETWEEN v.start_date AND v.end_date
WHERE rn = 1;