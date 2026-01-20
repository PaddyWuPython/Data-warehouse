#!/bin/bash

# 1. 设置环境变量
APP="nev_safety"
HIVE_DB="nev_safety_ods"
HADOOP_CMD="/opt/module/hadoop/bin/hadoop"
HIVE_CMD="/opt/module/hive/bin/hive"

# 2. 处理日期参数
if [ -n "$1" ]; then
    do_date=$1
else
    do_date=`date -d "-1 day" +%F`
fi

echo "开始执行日期: $do_date 的数据装载任务..."

# 3. 定义日志表与 HDFS 源路径的映射关系
# 格式: "表名 HDFS源路径前缀"
# 注意：ods_log_vehicle_track_inc 是按天+小时分区的，这里演示处理逻辑
declare -A table_map
table_map=(
    ["ods_log_vehicle_sys_inc"]="/origin_data/sys_event/$do_date"
    ["ods_log_safety_risk_inc"]="/origin_data/safety_risk/$do_date"
    ["ods_log_charging_session_inc"]="/origin_data/charging_session/$do_date"
    ["ods_log_app_feedback_inc"]="/origin_data/app_feedback/$do_date"
)

# 4. 加载普通按天分区的表
for table in "${!table_map[@]}"; do
    path=${table_map[$table]}
    
    # 检查HDFS路径是否存在
    $HADOOP_CMD fs -test -e $path
    if [ $? -eq 0 ]; then
        sql="ALTER TABLE $HIVE_DB.$table ADD IF NOT EXISTS PARTITION (dt='$do_date') LOCATION '$path';"
        echo "正在加载表: $table..."
        $HIVE_CMD -e "$sql"
        if [ $? -eq 0 ]; then
            echo "表 $table 加载成功."
        else
            echo "ERROR: 表 $table 加载失败!"
            exit 1
        fi
    else
        echo "WARNING: 路径 $path 不存在，跳过表 $table 的加载."
    fi
done

# 5. 特殊处理：ods_log_vehicle_track_inc (双分区 dt + hr)
# 假设 Flume 落盘路径为 /origin_data/vehicle_track/2023-10-27/00... /23
echo "开始加载核心轨迹表 ods_log_vehicle_track_inc..."

track_base_path="/origin_data/vehicle_track/$do_date"
$HADOOP_CMD fs -test -e $track_base_path
if [ $? -eq 0 ]; then
    # 循环 00 到 23 小时
    for i in {00..23}; do
        hr_path="$track_base_path/$i"
        $HADOOP_CMD fs -test -e $hr_path
        if [ $? -eq 0 ]; then
            sql="ALTER TABLE $HIVE_DB.ods_log_vehicle_track_inc ADD IF NOT EXISTS PARTITION (dt='$do_date', hr='$i') LOCATION '$hr_path';"
            $HIVE_CMD -e "$sql"
        else
            echo "WARNING: 小时路径 $hr_path 不存在."
        fi
    done
    echo "核心轨迹表加载完成."
else
    echo "WARNING: 核心轨迹表当天路径 $track_base_path 不存在."
fi

echo "任务完成: $do_date 数据装载结束."
