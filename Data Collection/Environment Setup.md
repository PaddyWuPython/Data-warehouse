# Environment setup
采集目标涵盖：车辆状态日志、行车事件、充电记录、电池异常等。使用组件包括：Flume、Kafka、Maxwell、DataX、HDFS、Hive。
链路A（日志/事件）：
车端/平台日志文件 → Flume(taildir/spooldir/exec) → Kafka(topic_*) → Flume(kafka source) → HDFS(ODS)

链路B（业务库CDC）：
MySQL业务库 → Maxwell(binlog) → Kafka(topic_db/topic_safety_db) → Flume(kafka source) → HDFS(ODS)

链路C（维表/全量批量）：
MySQL维表/历史 → DataX → HDFS/Hive(ODS维表)

## Cluster overview
| 节点名称 | 功能组件 |
|----------|-----------|
| node01   | Hadoop NN、Kafka、Hive、Flume、Maxwell、DataX |
| node02   | Hadoop DN、Kafka、Flume、DataX |
| node03   | Hadoop DN、Kafka、Flume、DataX |

## Configuration and Installation
节点：node01,node02,node03
目录：/home/module
```bash
export JAVA_HOME=/home/module/jdk8
export HADOOP_HOME=/home/module/hadoop
export HIVE_HOME=/home/module/hive
export ZK_HOME=/home/module/zookeeper
export KAFKA_HOME=/home/module/kafka
export FLUME_HOME=/home/module/flume
export DATAX_HOME=/home/module/datax
export PATH=$PATH:$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$HIVE_HOME/bin:$ZK_HOME/bin:$KAFKA_HOME/bin:$FLUME_HOME/bin
```

### Hadoop/HDFS
```shell
start-dfs.sh
start-yarn.sh
hdfs dfs -mkdir -p /warehouse/ods
hdfs dfs -mkdir -p /warehouse/ods/safety
hdfs dfs -chmod -R 775 /warehouse/ods
```

### Zookeeper and Kafka
```shell
kafka-server-start.sh -daemon $KAFKA_HOME/config/server.properties
kafka-topics.sh --bootstrap-server node01:9092 --list

```shell
kafka-topics.sh --bootstrap-server node01:9092 --create --topic topic_drive_status --partitions 6 --replication-factor 2

# 行车事件（急刹/超速/碰撞预警等）
kafka-topics.sh --bootstrap-server node01:9092 --create --topic topic_drive_event --partitions 6 --replication-factor 2

# 充电会话与充电事件日志
kafka-topics.sh --bootstrap-server node01:9092 --create --topic topic_charge_session --partitions 6 --replication-factor 2
kafka-topics.sh --bootstrap-server node01:9092 --create --topic topic_charge_event --partitions 6 --replication-factor 2

# 业务库CDC统一topic
kafka-topics.sh --bootstrap-server node01:9092 --create --topic topic_safety_db --partitions 6 --replication-factor 2

```

### Maxwell metadata storage
```sql
-- 1) maxwell元数据库
CREATE DATABASE IF NOT EXISTS maxwell DEFAULT CHARACTER SET utf8mb4;

-- 2) maxwell用户
CREATE USER 'maxwell'@'%' IDENTIFIED BY 'maxwell123';
GRANT ALL PRIVILEGES ON maxwell.* TO 'maxwell'@'%';
GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'maxwell'@'%';
FLUSH PRIVILEGES;
```

### Flume
```shell
export JAVA_HOME=/home/module/jdk8
python $DATAX_HOME/bin/datax.py --version
```