DROP TABLE IF EXISTS dim_user_zip;
CREATE EXTERNAL TABLE dim_user_zip (
    `user_sk` STRING,
    `user_id` STRING,
    `user_name_mask` STRING COMMENT '脱敏姓名',
    `gender` STRING,
    `age` INT,
    `age_group` STRING COMMENT '年龄段',
    `license_level` STRING,
    `register_city` STRING,
    `start_date` STRING,
    `end_date` STRING
)
COMMENT '用户画像历史拉链表'
STORED AS ORC;

-- 首日装载
INSERT OVERWRITE TABLE dim_user_zip
SELECT 
    md5(concat(user_id, '${do_date}')) as user_sk,
    user_id,
    -- 姓名脱敏
    concat(substr(user_name, 1, 1), '**') as user_name_mask,
    CASE gender WHEN 1 THEN '男' WHEN 2 THEN '女' ELSE '未知' END as gender,
    age,
    -- 计算年龄段
    CASE 
        WHEN age < 20 THEN '<20'
        WHEN age BETWEEN 20 AND 30 THEN '20-30'
        WHEN age BETWEEN 31 AND 40 THEN '31-40'
        WHEN age > 40 THEN '>40'
        ELSE 'Unknown'
    END as age_group,
    license_level,
    register_city,
    '${do_date}' as start_date,
    '9999-12-31' as end_date
FROM ods_biz_user_master_full
WHERE dt='${do_date}'

-- 日常增量装载
-- 准备今日全量视图
WITH ods_user_today AS (
SELECT 
    user_id,
    CASE 
        WHEN user_name IS NULL THEN 'Unknown'
        WHEN length(user_name) <= 1 THEN concat(user_name, '*')
        ELSE concat(substr(user_name, 1, 1), '**') 
    END AS user_name_mask,
    CASE 
        WHEN gender = 1 THEN '男' 
        WHEN gender = 2 THEN '女' 
        ELSE '未知' 
    END AS gender,
    COALESCE(age, -1) AS age,
    CASE 
        WHEN age IS NULL OR age < 0 THEN 'Unknown'
        WHEN age < 18 THEN '<18'
        WHEN age >= 18 AND age <= 25 THEN '18-25'
        WHEN age >= 26 AND age <= 35 THEN '26-35'
        WHEN age >= 36 AND age <= 45 THEN '36-45'
        WHEN age >= 46 AND age <= 60 THEN '46-60'
        ELSE '>60' 
    END AS age_group,
    COALESCE(license_level, 'Unknown') AS license_level,
    COALESCE(register_city, 'Unknown') AS register_city
    FROM ods_biz_user_master_full
    WHERE dt = '${do_date}'
),
dim_active AS (
    SELECT * 
    FROM dim_user_zip 
    WHERE end_date = '9999-12-31'
),
dim_history AS (
    SELECT * 
    FROM dim_user_zip 
    WHERE end_date < '9999-12-31'
)
INSERT OVERWRITE TABLE dim_user_zip 
-- 历史过期记录 -> 原样保留
SELECT * FROM dim_history
UNION ALL
-- 将发生变化的旧记录，End_Date 更新为昨天
SELECT 
    old.user_sk,
    old.user_id,
    old.user_name_mask,
    old.gender,
    old.age,
    old.age_group,
    old.license_level,
    old.register_city,
    old.start_date,
    '${yesterday}' AS end_date --修改有效期截止为昨天
FROM dim_active old
JOIN ods_today new ON old.user_id = new.user_id
WHERE 
    -- 检测任何一个业务字段是否发生变化
    old.age <> new.age OR
    old.license_level <> new.license_level OR 
    old.register_city <> new.register_city OR
    old.gender <> new.gender

UNION ALL
-- 新增/变更记录 -> 作为新条目插入，Start_Date 为今天
SELECT 
    md5(concat(new.user_id, '${do_date}')) AS user_sk, -- 重新生成SK
    new.user_id,
    new.user_name_mask,
    new.gender,
    new.age,
    new.age_group,
    new.license_level,
    new.register_city,
    '${do_date}' AS start_date, --!!! 开始时间为今天!!!
    '9999-12-31' AS end_date
FROM ods_today new
LEFT JOIN dim_active old ON new.user_id = old.user_id
WHERE 
    old.user_id IS NULL -- 
    OR (                -- 变更用户
        old.gender <> new.gender OR
        old.age <> new.age OR
        old.license_level <> new.license_level OR
        old.register_city <> new.register_city
    )

UNION ALL
-- 未变更的活跃记录 -> 原样保留
SELECT 
    old.user_sk,
    old.user_id,
    old.user_name_mask,
    old.gender,
    old.age,
    old.age_group,
    old.license_level,
    old.register_city,
    old.start_date,
    old.end_date
FROM dim_active old
JOIN ods_today new ON old.user_id = new.user_id
WHERE 
    old.gender = new.gender AND
    old.age = new.age AND
    old.license_level = new.license_level AND
    old.register_city = new.register_city;