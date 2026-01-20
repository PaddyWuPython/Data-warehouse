DROP TABLE IF EXISTS ods_log_vehicle_track_inc;
CREATE EXTERNAL TABLE ods_log_vehicle_track_inc (
    `common` STRUCT<
        vin: STRING COMMENT '车辆VIN码',
        iccid: STRING COMMENT 'SIM卡号',
        msg_id: STRING COMMENT '报文唯一ID',
        collect_time: BIGINT COMMENT '采集时间戳(ms)',
        receive_time: BIGINT COMMENT '接收时间戳(ms)'
    > COMMENT '公共头部信息',
    
    `vehicle` STRUCT<
        status: INT COMMENT '车辆状态:1启动,2熄火',
        charge_status: INT COMMENT '充电状态:1停车充电,2行驶充电,3未充电',
        run_mode: INT COMMENT '运行模式:1纯电,2混动',
        speed: DOUBLE COMMENT '车速(km/h)',
        odometer: DOUBLE COMMENT '累计里程(km)',
        total_voltage: DOUBLE COMMENT '总电压(V)',
        total_current: DOUBLE COMMENT '总电流(A)',
        soc: DOUBLE COMMENT 'SOC(%)',
        insulation_r: INT COMMENT '绝缘电阻(kΩ)',
        dc_status: INT COMMENT 'DC-DC状态'
    > COMMENT '整车数据',
    
    `motors` ARRAY<STRUCT<
        seq: INT COMMENT '电机序号',
        status: INT COMMENT '电机状态',
        rpm: DOUBLE COMMENT '转速',
        torque: DOUBLE COMMENT '转矩',
        temp: DOUBLE COMMENT '电机温度',
        ctrl_temp: DOUBLE COMMENT '控制器温度',
        bus_vol: DOUBLE COMMENT '母线电压',
        bus_curr: DOUBLE COMMENT '母线电流'
    >> COMMENT '驱动电机列表(支持多电机)',
    
    `location` STRUCT<
        lon: DOUBLE COMMENT '经度',
        lat: DOUBLE COMMENT '纬度',
        alt: DOUBLE COMMENT '海拔',
        heading: DOUBLE COMMENT '航向角',
        gps_status: INT COMMENT '定位状态:0有效,1无效'
    > COMMENT '位置信息',
    
    `bms` STRUCT<
        pack_vol: DOUBLE COMMENT '电池包总电压',
        pack_curr: DOUBLE COMMENT '电池包总电流',
        soc: DOUBLE COMMENT 'SOC',
        sxh: DOUBLE COMMENT 'SOH健康度',
        max_vol_sys: INT COMMENT '最高电压子系统号',
        max_vol_code: INT COMMENT '最高电压单体代号',
        max_vol: DOUBLE COMMENT '最高单体电压值',
        min_vol: DOUBLE COMMENT '最低单体电压值',
        max_temp: DOUBLE COMMENT '最高温度值',
        min_temp: DOUBLE COMMENT '最低温度值'
    > COMMENT 'BMS主数据及极值',
    
    `cells` ARRAY<STRUCT<
        sys_seq: INT COMMENT '电池子系统号',
        vol_list: ARRAY<DOUBLE> COMMENT '单体电压列表'
    >> COMMENT '电池单体电压矩阵(核心热失控分析数据)',
    
    `temps` ARRAY<STRUCT<
        sys_seq: INT COMMENT '电池子系统号',
        probe_list: ARRAY<DOUBLE> COMMENT '探针温度列表'
    >> COMMENT '电池温度探针矩阵',
    
    `alarm` STRUCT<
        max_level: INT COMMENT '最高报警等级',
        alarm_bits: STRING COMMENT '通用报警标志位(二进制字符串)',
        fault_codes: ARRAY<STRING> COMMENT '故障码列表(Hex)'
    > COMMENT '实时报警数据',
    
    `charging` STRUCT<
        chg_vol: DOUBLE COMMENT '充电电压',
        chg_curr: DOUBLE COMMENT '充电电流',
        chg_temp: DOUBLE COMMENT '充电枪温度'
    > COMMENT '充电过程监控(仅充电状态有效)'
)
COMMENT '车辆实时综合轨迹日志表'
PARTITIONED BY (`dt` STRING COMMENT '日期分区', `hr` STRING COMMENT '小时分区')
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
WITH SERDEPROPERTIES (
    'ignore.malformed.json' = 'true'
)
STORED AS TEXTFILE
LOCATION '/warehouse/nev_safety/ods/ods_log_vehicle_track_inc/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');