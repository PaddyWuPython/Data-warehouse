# Data Collection process
## Definition of data
1. 车辆状态日志（高频）
粒度：粒度：车-时间戳
核心字段：vehicle_id, ts, speed, mileage, soc, total_voltage, total_current, lat, lon, gear, brake, steering_angle …

2. 行车事件日志（低频事件）
急刹/急加速/超速/碰撞预警/车道偏离等
字段：vehicle_id, event_time, event_type, severity, value, lat, lon, extra_json

3. 充电会话（会话级） topic_charge_session
字段：session_id, vehicle_id, start_time, end_time, energy_kwh, start_soc, end_soc, station_id, charge_type …
4. 充电过程事件（分钟级/异常事件）
过温/过流/断枪/接触不良/异常终止等

5. 业务库CDC（事故/工单/充电订单等）
统一 CDC topic：`topic_safety_db`

## Logs
车端/平台产生日志文件
- 车辆状态落文件：/data/safety/log/drive_status/*.log
- 行车事件落文件：/data/safety/log/drive_event/*.log
- 充电事件落文件：/data/safety/log/charge_event/*.log

采集方式：Flume TAILDIR Source
流程：
- 应用写日志到本地目录（json line）
- Flume Agent 监听目录增量
- 发送到 Kafka Topic

```shell
#!/bin/bash
nohup $FLUME_HOME/bin/flume-ng agent \
  -n a1 \
  -c $FLUME_HOME/conf \
  -f $FLUME_HOME/conf/safety_log_to_kafka.conf \
  -Dflume.root.logger=INFO,LOGFILE \
  > /data/flume/logs/safety_log_to_kafka.out 2>&1 &
```

```shell
#!/bin/bash
nohup $FLUME_HOME/bin/flume-ng agent \
  -n a2 \
  -c $FLUME_HOME/conf \
  -f $FLUME_HOME/conf/kafka_to_hdfs_ods.conf \
  -Dflume.root.logger=INFO,LOGFILE \
  > /data/flume/logs/kafka_to_hdfs_ods.out 2>&1 &

```

## MySQL Data
- MySQL 开启 binlog (ROW)
- Maxwell 作为 binlog consumer
- 输出到 Kafka topic_safety_db
- 下游 Flume 从 Kafka 落地 HDFS（ODS）

## DataX
- dim_vehicle、dim_station、历史充电会话归档、历史事故全量等
- 每日凌晨跑一次：MySQL → HDFS(ODS)（按 dt 分区）

## Scripts for runing tasks
- task start
```shell
#!/bin/bash
bash /home/script/maxwell_start.sh
bash /home/script/flume_start_safety_log_to_kafka.sh
bash /home/script/flume_start_kafka_to_hdfs_ods.sh
echo "Safety pipeline all started."
```
- task stop
```shell
#!/bin/bash
ps -ef | grep -E "maxwell|flume-ng agent" | grep -v grep | awk '{print $2}' | xargs -r kill -9
```