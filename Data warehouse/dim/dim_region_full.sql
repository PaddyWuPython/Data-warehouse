DROP TABLE IF EXISTS dim_region_full;
CREATE EXTERNAL TABLE dim_region_full (
    `region_id` STRING COMMENT '行政区划码(主键,如330106)',
    `region_name` STRING COMMENT '行政区名称(如西湖区)',
    `province_id` STRING COMMENT '所属省份ID',
    `province_name` STRING COMMENT '所属省份名称',
    `city_id` STRING COMMENT '所属城市ID',
    `city_name` STRING COMMENT '所属城市名称',
    `district_id` STRING COMMENT '所属区县ID',
    `district_name` STRING COMMENT '所属区县名称',
    `region_level` INT COMMENT '行政级别: 1省, 2市, 3区县',
    `full_name` STRING COMMENT '全称(如: 浙江省杭州市西湖区)'
)
COMMENT '行政地区维度表'
STORED AS ORC
TBLPROPERTIES ("orc.compress"="SNAPPY");

-- 数据装载
INSERT OVERWRITE TABLE dim_region_full
SELECT 
    -- 最终的 region_id 取决于当前记录的级别
    COALESCE(district.id, city.id, province.id) AS region_id,
    
    -- 自身名称
    COALESCE(district.name, city.name, province.name) AS region_name,
    
    -- 省份信息
    province.id AS province_id,
    province.name AS province_name,
    
    -- 城市信息 (如果是省份级，则为空或用-1填充)
    NVL(city.id, '-1') AS city_id,
    NVL(city.name, 'N/A') AS city_name,
    
    -- 区县信息
    NVL(district.id, '-1') AS district_id,
    NVL(district.name, 'N/A') AS district_name,
    
    -- 级别
    COALESCE(district.level, city.level, province.level) AS region_level,
    
    -- 拼接全称 (处理不同级别的情况)
    CASE 
        WHEN district.id IS NOT NULL THEN CONCAT(province.name, city.name, district.name)
        WHEN city.id IS NOT NULL THEN CONCAT(province.name, city.name)
        ELSE province.name 
    END AS full_name

FROM 
    -- 先取省级数据 
    (SELECT * FROM ods_base_region_info_full WHERE level = 1) province
    
    -- 左连接市级数据
    LEFT JOIN (SELECT * FROM ods_base_region_info_full WHERE level = 2) city 
    ON province.id = city.parent_id
    
    -- 左连接区县级数据
    LEFT JOIN (SELECT * FROM ods_base_region_info_full WHERE level = 3) district 
    ON city.id = district.parent_id;