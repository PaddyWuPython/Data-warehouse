DROP TABLE IF EXISTS ods_biz_vehicle_owner_rel_full;
CREATE EXTERNAL TABLE ods_biz_vehicle_owner_rel_full (
    `id` STRING COMMENT '关系ID',
    `vin` STRING COMMENT '车辆VIN',
    `user_id` STRING COMMENT '用户ID',
    `relation_type` INT COMMENT '关系类型: 1车主, 2授权驾驶员',
    `bind_time` STRING COMMENT '绑定时间',
    `unbind_time` STRING COMMENT '解绑时间',
    `is_valid` INT COMMENT '是否有效: 0失效, 1有效'
)
COMMENT '人车关系全量表'
PARTITIONED BY (`dt` STRING)
STORED AS ORC
TBLPROPERTIES ("orc.compress"="SNAPPY");