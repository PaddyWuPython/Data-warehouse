DROP TABLE IF EXISTS dim_vehicle_zip;
CREATE EXTERNAL TABLE dim_vehicle_zip (
    `vehicle_sk` STRING COMMENT '车辆代理键(MD5:vin+start_date)',
    `vin` STRING COMMENT '车辆VIN码',
    -- 基础属性 (来自 vehicle_master)
    `plate_no` STRING COMMENT '车牌号',
    `production_date` STRING COMMENT '生产日期',
    `sales_date` STRING COMMENT '销售日期',
    `color` STRING COMMENT '车身颜色',
    `region_name` STRING COMMENT '注册地区名称',
    -- 车型属性 (来自 vehicle_model)
    `model_name` STRING COMMENT '车型名称',
    `series_name` STRING COMMENT '车系名称',
    `vehicle_type` STRING COMMENT '车辆类型',
    `power_type` STRING COMMENT '动力类型(BEV/PHEV)',
    `battery_type` STRING COMMENT '电池材料类型(三元/铁锂)',
    `nedc_range` INT COMMENT 'NEDC续航(km)',
    -- 管理属性 (来自 owner_rel, sim_card)
    `owner_id` STRING COMMENT '当前车主ID',
    `is_valid_owner` INT COMMENT '车主关系是否有效',
    `iccid` STRING COMMENT 'SIM卡号',
    `sim_operator` STRING COMMENT '运营商',
    -- 拉链时间
    `start_date` STRING COMMENT '生效开始日期',
    `end_date` STRING COMMENT '生效结束日期(9999-12-31为当前有效)'
)
COMMENT '车辆全量历史拉链表'
STORED AS ORC
TBLPROPERTIES ("orc.compress"="SNAPPY");

-- 首日装载
INSERT OVERWRITE TABLE dim_vehicle_zip
SELECT 
    md5(concat(v.vin, '2020-01-01')) as vehicle_sk, -- 初始SK
    v.vin,
    v.plate_no,
    v.production_date,
    v.sales_date,
    v.color,
    v.region_code,
    m.model_name,
    m.series_name,
    'Passenger Car' as vehicle_type,
    m.motor_type as power_type,
    m.battery_type,
    m.nedc_range,
    o.user_id as owner_id,
    1 as is_valid_owner,
    s.iccid,
    s.operator as sim_operator,
    '2020-01-01' as start_date,
    '9999-12-31' as end_date
FROM ods_biz_vehicle_master_full v
LEFT JOIN ods_biz_vehicle_model_full m ON v.model_id = m.model_id AND m.dt='${do_date}'
LEFT JOIN ods_biz_vehicle_owner_rel_full o ON v.vin = o.vin AND o.dt='${do_date}' AND o.is_valid=1
LEFT JOIN ods_biz_sim_card_full s ON v.vin = s.vin AND s.dt='${do_date}'
WHERE v.dt='${do_date}';

-- 日常增量装载

-- 准备今日全量视图
WITH ods_vehicle_today AS (
    SELECT 
        v.vin,
        v.plate_no, v.production_date, v.sales_date, v.color, v.region_code,
        m.model_name, m.series_name, m.motor_type, m.battery_type, m.nedc_range,
        o.user_id, s.iccid, s.operator
    FROM ods_biz_vehicle_master_full v
    LEFT JOIN ods_biz_vehicle_model_full m ON v.model_id = m.model_id AND m.dt='${do_date}'
    LEFT JOIN ods_biz_vehicle_owner_rel_full o ON v.vin = o.vin AND o.dt='${do_date}' AND o.is_valid=1
    LEFT JOIN ods_biz_sim_card_full s ON v.vin = s.vin AND s.dt='${do_date}'
    WHERE v.dt='${do_date}'
),

-- 取出昨日拉链表中的“当前有效”记录
dim_vehicle_active AS (
    SELECT * FROM dim_vehicle_zip WHERE end_date = '9999-12-31'
),

-- 取出昨日拉链表中的“历史过期”记录 (无需变动，直接保留)
dim_vehicle_history AS (
    SELECT * FROM dim_vehicle_zip WHERE end_date < '9999-12-31'
)

-- 合并逻辑
INSERT OVERWRITE TABLE dim_vehicle_zip
-- 历史过期记录 -> 原样保留
SELECT * FROM dim_vehicle_history

UNION ALL

-- 将发生变化的旧记录，End_Date 更新为昨天
SELECT 
    old.vehicle_sk, old.vin, old.plate_no, old.production_date, old.sales_date, old.color, old.region_name,
    old.model_name, old.series_name, old.vehicle_type, old.power_type, old.battery_type, old.nedc_range,
    old.owner_id, old.is_valid_owner, old.iccid, old.sim_operator,
    old.start_date, 
    '${yesterday}' as end_date --!!! 截断日期!!!
FROM dim_vehicle_active old
JOIN ods_vehicle_today new ON old.vin = new.vin
WHERE 
    -- 只要有一个字段不同，就视为变更
    nvl(old.plate_no,'') <> nvl(new.plate_no,'') OR 
    nvl(old.owner_id,'') <> nvl(new.user_id,'') OR
    nvl(old.iccid,'') <> nvl(new.iccid,'')

UNION ALL

-- 新增/变更记录 -> 作为新条目插入，Start_Date 为今天
SELECT 
    md5(concat(new.vin, '${do_date}')) as vehicle_sk,
    new.vin, new.plate_no, new.production_date, new.sales_date, new.color, new.region_code,
    new.model_name, new.series_name, 'Passenger Car', new.motor_type, new.battery_type, new.nedc_range,
    new.user_id, 1, new.iccid, new.operator,
    '${do_date}' as start_date, --!!! 开始日期!!!
    '9999-12-31' as end_date
FROM ods_vehicle_today new
LEFT JOIN dim_vehicle_active old ON new.vin = old.vin
WHERE 
    old.vin IS NULL -- 新增车辆
    OR ( -- 变更车辆
        nvl(old.plate_no,'') <> nvl(new.plate_no,'') OR 
        nvl(old.owner_id,'') <> nvl(new.user_id,'') OR
        nvl(old.iccid,'') <> nvl(new.iccid,'')
    )

UNION ALL

-- 未变更的活跃记录 -> 原样保留
SELECT 
    old.*
FROM dim_vehicle_active old
JOIN ods_vehicle_today new ON old.vin = new.vin
WHERE 
    nvl(old.plate_no,'') = nvl(new.plate_no,'') AND 
    nvl(old.owner_id,'') = nvl(new.user_id,'') AND
    nvl(old.iccid,'') = nvl(new.iccid,'');