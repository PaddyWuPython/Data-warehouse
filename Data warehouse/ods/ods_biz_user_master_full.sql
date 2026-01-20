DROP TABLE IF EXISTS ods_biz_user_master_full;
CREATE EXTERNAL TABLE ods_biz_user_master_full (
    `user_id` STRING COMMENT '用户ID',
    `user_name` STRING COMMENT '用户姓名(脱敏)',
    `gender` INT COMMENT '性别: 0未知, 1男, 2女',
    `age` INT COMMENT '年龄',
    `register_city` STRING COMMENT '注册城市',
    `license_level` STRING COMMENT '驾照等级',
    `register_time` STRING COMMENT '注册时间'
)
COMMENT '用户信息全量表'
PARTITIONED BY (`dt` STRING)
STORED AS ORC
TBLPROPERTIES ("orc.compress"="SNAPPY");