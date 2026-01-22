DROP TABLE IF EXISTS dim_charging_pile_full;
CREATE EXTERNAL TABLE dim_charging_pile_full (
    `pile_id` STRING COMMENT '充电桩ID',
    `pile_name` STRING COMMENT '充电桩名称',
    `station_id` STRING COMMENT '所属场站ID',
    `station_name` STRING COMMENT '场站名称',
    `operator_name` STRING COMMENT '运营商名称(映射后)',
    `pile_type` STRING COMMENT '桩类型(DC快充/AC慢充)',
    `power_rating` DOUBLE COMMENT '额定功率(kW)',
    `voltage_platform` INT COMMENT '电压平台(V)',
    `province` STRING COMMENT '所在省份',
    `city` STRING COMMENT '所在城市',
    `geo_location` STRING COMMENT '经纬度(lon,lat)'
)
COMMENT '充电设施统一维度表'
PARTITIONED BY (`dt` STRING)
STORED AS ORC;

INSERT OVERWRITE TABLE dim_charging_pile_full PARTITION(dt='${do_date}')
SELECT 
    p.pile_id,
    nvl(p.pile_id, 'Unknown') as pile_name,
    s.station_id,
    s.station_name,
    CASE s.operator_id 
        WHEN '1001' THEN '特来电' 
        WHEN '1002' THEN '国家电网' 
        ELSE '其他' 
    END as operator_name,
    CASE p.connector_type 
        WHEN 1 THEN 'DC' 
        WHEN 2 THEN 'AC' 
        ELSE 'Unknown' 
    END as pile_type,
    p.power_rating,
    p.voltage_platform,
    -- region_code 前2位是省，前4位是市
    substr(s.region_code, 1, 2) as province,
    substr(s.region_code, 1, 4) as city,
    concat(cast(s.lon as string), ',', cast(s.lat as string)) as geo_location
FROM ods_biz_charging_pile_full p
JOIN ods_biz_charging_station_full s ON p.station_id = s.station_id AND s.dt='${do_date}'
WHERE p.dt='${do_date}';