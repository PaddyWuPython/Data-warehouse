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

**※**有一张用户登录日志表ods_login_usr_log, 包含user_id（用户id）和login_dt（登录日期）
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

有一张用户余额表dwd_fnd_bal_usr_dd, 包含user_id（用户id）、bal（当前余额）以及dt（日期）
问题：请补全所有用户的缺失余额


select 
    user_id,
    sum(bal) over(partition by user_id, group_id order by dt) as bal  
from
(select 
    user_id,
    bal,
    dt,
    sum(if(bal, 1, 0) over(partition by user_id order by dt)) as group_id  
from 
    dwd_fnd_bal_usr_dd) t 
;
-- 第二方案
select 
    user_id,
    last_value(bal, true) over(partition by user_id order by dt 
        rows between unbounded preceding and current row) as bal 
from 
    dwd_fnd_bal_usr_dd;

有一张股票交易价格表ods_stock_trd_log, 包含sto_code（股票代码）、trade_dt（交易日期）以及price（交易价格）
问题：求出每只股票对应的波峰和波谷
波峰：股票价格高于前一天和后一天时
波谷：股票价格低于前一天和后一天时

select 
    sto_code,
    trade_dt 
    case 
        when price > lag(price, 1, null) over(partition by sto_code order by trade_dt) 
             and price > lead(price, 1, null) over(partition by sto_code order by trade_dt) 
        then 'peak'
        when price < lag(price, 1, null) over(partition by sto_code order by trade_dt) 
             and price < lead(price, 1, null) over(partition by sto_code order by trade_dt) 
        then 'valley'
        else 'normal'
    end as price_type
from 
    ods_stock_trd_log


有一张用户行为日志表ods_usr_log, 包含user_id（用户id）、start_time（登录时间）以及end_time（注销时间）
问题：求出用户登录的所有最大时间段，比如用户1在10:00登录且11:00注销，用户2在10:30登录且12:00注销，那么最大时间段就是 10:00到12:00

select 
    grp_id,
    min(start_time) as start_time,
    max(end_time) as end_time 
from 
(
    select 
        start_time,
        end_time,
        sum(flag) over(order by start_time, end_time) as grp_id
    from 
    (select 
        start_time,
        end_time,
        CASE WHEN start_time <= MAX(end_time) OVER (
                ORDER BY start_time, end_time 
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ) THEN 0 ELSE 1 END AS flag 
    from 
        ods_usr_log) t
    ) t1 
group by grp_id;

有一张美团app用户登录表dwd_mt_user_login_dd，包括字段：uid（用户id），login_dt（登录日期）
问题：计算每日新用户登录的次日留存率

with new_user_tb as (
    select 
        uid,
        min(login_dt) as register_dt 
    from 
        dwd_mt_user_login_dd 
) 
select 
    a.login_dt,
    count(distinct case when datediff(b.login_dt, a.login_dt) = 1 then b.uid else null end) / count(distinct a.uid) as ret_rate_1d 
from 
    new_user_tb a
left join 
    dwd_mt_user_login_dd b 
on 
    a.uid = b.uid
group by a.login_dt;

有一张骑手跑单汇总表dws_mt_qs_order_dd，包含qs_id（骑手id）、order_cnt（跑单量）、snap_dt（统计日期）
问题：计算近1个月每个外卖员跑单的中位数是多少
注意：如果外卖员当天没跑单，不在中位数统计范围内；


select 
    qs_id,
    order_count 
from 
(
    select 
        qs_id,
        order_count,
        count(*) over(partition by qs_id) as cnt,
        row_number() over(partition by qs_id order by order_count) as rn_asc,
        row_number() over(partition by qs_id order by order_count desc) as rn_desc 
    from 
        dws_mt_qs_order_dd
    where 
        snap_dt >= date_sub(current_date, 30)
) t 
where rn_asc >= cnt / 2 and rn_desc >= cnt / 2;

有一张美团app登录日志表ods_mt_login_usr_delta, 包含user_id（用户id）、dt（登录日期）

with register_tb as (
    select 
        user_id,
        min(dt) as register_dt 
    from 
        ods_mt_login_usr_delta 
    group by user_id
)
select 
    a.dt,
    count(distinct case when a.dt = register_dt then a.user_id else null end) as new_user_cnt 
from 
    ods_mt_login_usr_delta a 
left join 
    register_tb b
on 
    a.user_id = b.user_id 
group by a.dt;

有一张百度app用户信息表ods_bd_user_log_di，包括字段：id（主键id），name（姓名）

问题：对相同姓名的数据进行压缩存储【规则如下: 如果连续遇到相同的姓名，仅保存最小的id，以及姓名的集合】

select 
    min(id) as id,
    group_concat(name) as name_list 
from (
    select 
        id,
        name,
        sum(case 
            when name = lag(name, 1, null) over(order by id) then 0 
            else 1 
        end) as grp_id 
    from 
        ods_bd_user_log_di
) t 
GROUP BY grp_id;

(hard)已知一张用户行为日志表tb_user_log，字段包括uid-用户ID、artical_id-文章ID、in_time-进入时间、out_time-离开时间、sign_in-是否签到
注意1：只有artical_id为0时sign_in值才有效；
注意2：从2021年7月7日0点开始，用户每天签到可以领1金币，并可以开始累积签到天数，连续签到的第3、7天分别可额外领2、6金币，每连续签到7天后重新累积签到天数
问题：计算每个用户2021年7月至10月每月获得的金币数

select 
    uid,
    month(dt) as month,
    sum(coins_per_day) as coin 
from (
    select 
        uid,
        date_sub(dt, interval rn day) as dt_grp,
        case row_number() over(partition by uid,date_sub(dt, interval rn day) order by dt) % 7 
            when 3 then 3 
            when 0 then 7 
            else 1
        end as coins_per_day 
    from 
    (
        select 
            distinct uid,
            date(in_time) as dt,
            row_number() over(partition by uid order by date(in_time)) as rn 
        from 
            tb_user_log
        where 
            artical_id = 0 
            and sign_in = 1 
            and date(in_time) >= '2021-07-01' 
            and date(in_time) < '2021-11-01'
    ) t
) t 
group by uid, month(dt);

(classic)有一张微信好友关系表dwd_wx_user_friend_mapping_dd，包括字段：uid（用户id），fid（好友id）；还有一张用户运动步数表dwd_wx_user_step_di，包括字段：uid（用户id），steps（步数）

select uid, steps, rn from 
(select 
    t.uid,
    t.fid,
    s.steps,
    row_number() over(partition by t.uid order by s.steps desc) as rn
(
    select 
        uid, fid 
    from 
        dwd_wx_user_friend_mapping_dd 
    union all 
    select 
        uid, uid as fid 
    from 
        dwd_wx_user_friend_mapping_dd 
    group by uid
) t 
left join 
    dwd_wx_user_step_di s 
on 
    t.fid = s.uid ) t 
where uid = fid;


有一张用户登录日志表ods_usr_login_log, 包含user_id（用户id）、ds（登录时间）以及stay_time（停留时长，单位:ms）
问题：计算每个用户每天最后一次登录的停留时长

select 
    user_id,
    dt,
    stay_time 
from (
    select 
        user_id,
        date(ds) as dt,
        stay_time,
        row_number() over(partition by user_id, date(ds) order by ds desc) as rn 
    from 
        ods_usr_login_log
) t 
where rn = 1;

给定三个数据表：
user_attr：用户兴趣(10亿)，包含user_id（bigint），platform（bigint），interest_tag_id（int）
user_view_act：用户行为表（100亿），包含user_id（bigint），action_time（bigint）, action（int）
interest_tag_dim：兴趣id-名称映射维表（1万），包含interest_tag_id（int）, interest_name（string）
其中user_view_act属于原始用户行为表，未做任何数据清洗和异常处理，action_time精确到秒级。
问题：请编写hiveSQL，使用join实现高效数据关联，需要考虑数据倾斜问题；输出user_id, action维度中间表，包含：user_id, action, platform, interest_tag_id, interest_name字段。

-- bucket solution
ALTER TABLE user_attr CLUSTERED BY (user_id) SORTED BY (user_id) INTO 1024 BUCKETS;
ALTER TABLE user_view_act CLUSTERED BY (user_id) SORTED BY (user_id) INTO 1024 BUCKETS;
select 
  /*+mapjoin(t4)*/
  t1.user_id,
  t1.action,
  t3.platform,
  t3.interest_tag_id,
  t4.interest_name
from user_view_act t1
join user_attr t3
on t1.user_id = t3.user_id
join interest_tag_dim t4
on t3.interest_tag_id = t4.interest_tag_id

-- 如果热点key的数据量较大
-- 此时拆分出来的热点key无法广播出去，那么就需要添加随机数打散到不同的reduce中
with rdkey as (
  select
    id_start + tmp.pos as rd_id
  from (
      select 1  as id_start, 10 as id_end
  ) t
  lateral view posexplode(split(space(id_end - id_start),'')) tmp as pos, val
),
-- 打散两个表
user_view_act1 as (
  select user_view_act.*, rd_id
  from user_view_act
  join rdkey
  on 1=1
),
user_attr1 as (
  select user_attr.*, rd_id
  from user_attr
  join rdkey
  on 1=1
)
select 
  /*+mapjoin(t4)*/
  t3.user_id,
  t3.action,
  t3.platform,
  t3.interest_tag_id,
  t4.interest_name
from (
  select  
    t1.user_id,
    t1.action,
    t2.platform,
    t2.interest_tag_id
  from user_view_act1 t1
  join user_attr1 t2
  on t1.user_id = t2.user_id
  and t1.rd_id = t2.rd_id
  group by 
    t1.user_id,
    t1.action,
    t2.platform,
    t2.interest_tag_id
)t3
join interest_tag_dim t4
on t3.interest_tag_id = t4.interest_tag_id;

(hard)有一张直播间用户进出日志表ods_ks_usr_io_log，包括如下字段：uid（用户id）、in_time（进入时间）、out_time（离开时间）

问题：计算分钟级直播在线人数

with recursive as (
    select 
        min(in_time) as time 
    from 
        ods_ks_usr_io_log 
    union all 
    select 
        time + interval 1 minute as time 
    from 
        recursive 
    where 
        time < (select max(out_time) from ods_ks_usr_io_log)
)
select 
    r.time,
    count(distinct u.uid) as online_user_cnt 
from 
    recursive r
left join
    ods_ks_usr_io_log u
on 
    r.time >= u.in_time 
    and r.time < u.out_time 
group by r.time;