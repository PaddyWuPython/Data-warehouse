DROP TABLE IF EXISTS dim_battery_pack_full;
CREATE EXTERNAL TABLE dim_battery_pack_full (
    `pack_code` STRING COMMENT '电池包编码',
    `vin` STRING COMMENT '当前关联VIN',
    `supplier_name` STRING COMMENT '电池包供应商名称',
    `cell_model` STRING COMMENT '电芯型号',
    `cell_count` INT COMMENT '电芯数量',
    `chemistry_system` STRING COMMENT '化学体系(NCM/LFP)',
    `pack_capacity` DOUBLE COMMENT '总容量(kWh)',
    `rated_voltage` DOUBLE COMMENT '额定电压(V)',
    `production_batch` STRING COMMENT '生产批次',
    `production_date` STRING COMMENT '生产日期'
)
COMMENT '电池包零部件维度表'
PARTITIONED BY (`dt` STRING)
STORED AS ORC
TBLPROPERTIES ("orc.compress"="SNAPPY");

-- 数据装载
INSERT OVERWRITE TABLE dim_battery_pack_full PARTITION(dt='${do_date}')
SELECT 
    bp.pack_code,
    bp.vin,
    nvl(sup.supplier_name, 'Unknown') as supplier_name,
    bp.cell_model,
    bp.cell_count,
    CASE 
        WHEN bp.cell_model LIKE '%LFP%' THEN 'LFP' 
        WHEN bp.cell_model LIKE '%NCM%' THEN 'NCM' 
        ELSE 'Other' 
    END as chemistry_system,
    -- 额定容量需计算或直接读取
    CAST(NULL AS DOUBLE) AS pack_capacity, 
    bp.rated_voltage,
    bp.production_batch,
    -- 从BOM中提取日期
    regexp_replace(substr(bp.production_batch, 1, 8), '(\\d{4})(\\d{2})(\\d{2})', '$1-$2-$3') as production_date
FROM ods_biz_battery_pack_full bp
LEFT JOIN ods_biz_supplier_info_full sup ON bp.supplier_id = sup.supplier_id AND sup.dt='${do_date}'
WHERE bp.dt='${do_date}';