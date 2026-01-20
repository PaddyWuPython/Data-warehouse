DROP TABLE IF EXISTS ods_biz_supplier_info_full;
CREATE EXTERNAL TABLE ods_biz_supplier_info_full (
    `supplier_id` STRING COMMENT '供应商ID',
    `supplier_name` STRING COMMENT '供应商名称',
    `component_type` STRING COMMENT '供应零件类型',
    `contact_info` STRING COMMENT '联系方式'
)
COMMENT '供应商信息全量表'
PARTITIONED BY (`dt` STRING)
STORED AS ORC
TBLPROPERTIES ("orc.compress"="SNAPPY");