有一张直播间用户进出日志表ods_ks_zb_usr_io_log, 包含uid（用户id）、op_time（进出时间）、op_type（进入=1，出去=0）、oid（直播间id）
问题：计算所有直播间峰值在线人数以及持续时间（单位：分钟）

-- 举例如下：
-- 输入
uid  op_time  op_type  oid
1001  11:11      1      A
1002  11:12      1      A
1002  11:13      0      A
1003  11:14      1      A
1005  11:19      1      A
1006  11:23      1      A
1001  11:30      0      A
-- 输出
oid  max_cnt  dura_ts
A      3        7 

select 
    oid,
    online_count as max_cnt,
    end_time - op_time as duration_minutes
from
(
    select 
        uid,
        op_time
        oid,
        online_count,
        max(online_count) over(partition by oid) as peak_online_count,
        lead(op_time, 1) over(partition by oid order by op_time) as end_time 
    from
    (select 
        uid,
        oid,
        op_time,
        sum(if(op_type = 1, 1, -1)) over (partition by oid order by op_time) as online_count 
    from 
        ods_ks_zb_usr_io_log) t
) t1 
where online_count = peak_online_count;

有一张若干妖股交易流水表dwd_trd_stock_price_log，包含s_code（股票代码）、price（交易价格）、ds（交易时间）
问题：计算每只股票收盘价持续上涨的最大天数
备注：当日最晚的交易时间即为收盘时间

-- 举例如下：
-- 输入
s_code   price        ds
001       100  2024-10-09 9:30
001       90   2024-10-09 10:00
001       95   2024-10-09 10:30
001       80   2024-10-09 11:00
001       90   2024-10-09 15:00
001       100  2024-10-10 15:00
001       120  2024-10-11 15:00
001       80   2024-10-12 15:00
001       100  2024-10-13 15:00
002       10   2024-10-09 9:30
002       20   2024-10-09 10:00
002       50   2024-10-09 10:30
002       20   2024-10-09 11:00
002       40   2024-10-09 15:00
002       30   2024-10-10 15:00
002       20   2024-10-11 15:00
002       10   2024-10-12 15:00
002       60   2024-10-13 15:00

-- 输出
s_code  max_up_cnt
001        2
002        1

WITH daily_price AS (
    -- 1. 去重，获取每日最后一次价格
    SELECT 
        s_code,
        DATE(ds) AS trade_date,
        price 
    FROM (
        SELECT 
            s_code,
            ds,
            price,
            ROW_NUMBER() OVER(PARTITION BY s_code, ds ORDER BY ds DESC) AS rn
        FROM dwd_trd_stock_price_log
    ) t
    WHERE rn = 1
)

SELECT 
    s_code,
    MAX(up_cnt) AS max_up_cnt 
FROM (
    -- 4. 按“岛屿”分组，计算每个连续上涨区间的长度
    SELECT 
        s_code,
        dt,
        COUNT(*) AS up_cnt 
    FROM (
        -- 3. 计算“日期 - 行号”的差值 dt，连续上涨的记录会得到相同的 dt
        SELECT 
            s_code,
            price,
            DATE_SUB(trade_date, ROW_NUMBER() OVER(PARTITION BY s_code ORDER BY trade_date)) AS dt 
        FROM (
            -- 2. 判断当天是否比后一天价格低（上涨预判）
            SELECT 
                s_code,
                price,
                trade_date,
                IF(price > LAG(price, 1, 9999) OVER(PARTITION BY s_code ORDER BY trade_date), 1, 0) AS is_up 
            FROM daily_price  
        ) t1
        WHERE is_up = 1
    ) t2
    GROUP BY s_code, dt
) t4 
GROUP BY s_code;

有一张部门员工信息表dwd_emp_info_dd，包含emp_id（用户id）、dept_name（部门名称）、salary（薪资）
问题：计算部门平均薪资（要求去除部门最高和最低工资）


select 
    dept_name,
    AVG(salary) as avg_salary
from
(
    select 
        emp_id,
        dept_name,
        salry,
        ROW_NUMBER() over(partition by dept_name order by salary asc) as rn_asc,
        ROW_NUMBER() over(partition by dept_name order by salary desc) as rn_desc 
    from
        dwd_emp_info_dd
) t
where rn_asc != 1 and rn_desc != 1 
group by dept_name;

有一张用户登录日志表ods_login_usr_log, 包含user_id（用户id）和login_dt（登录日期）
问题：计算每个用户最大的连续登录天数，可以间隔一天

select 
    user_id,
    max(max_cnt) as max_cnt 
from 
(
    select 
        user_id,
        max(login_dt) - min(login_dt) + 1 as max_cnt 
    from 
    (
        select 
            user_id,
            sum(if(diff_days > 2, 1, 0)) as grp_id 
        from
        (
            select 
                user_id,
                login_dt,
                date_sub(login_dt, lag(login_dt,1,0) over(partition by user_id order by login_dt)) as diff_days 
            from 
                ods_login_usr_log
        ) t
    ) t1 
    group by user_id, grp_id
) t2 
group by user_id;

 



  
