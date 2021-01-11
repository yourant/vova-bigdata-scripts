drop table dwd.dwd_vova_fact_start_up;
CREATE TABLE IF NOT EXISTS dwd.dwd_vova_fact_start_up
(
    datasource     string comment '数据平台',
    device_id     string COMMENT '设备ID',
    buyer_id      bigint COMMENT '设备对应用户ID',
    start_up_date date   COMMENT '设备启动日期',
    app_version   string COMMENT '设备安装app版本',
    platform      string COMMENT '设备平台',
    language_code string COMMENT '设备所选语言',
    region_code   string COMMENT 'geo_country',
    app_region_code   string COMMENT 'country',
    min_collector_time  TIMESTAMP COMMENT '当日登录最小时间',
    max_collector_time  TIMESTAMP COMMENT '当日登录最大时间'
) COMMENT '设备启动事实表' PARTITIONED BY (pt STRING,dp STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;


drop table tmp.tmp_vova_css_start_up;
CREATE TABLE IF NOT EXISTS tmp.tmp_vova_css_start_up
(
    datasource     string comment '数据平台',
    device_id     string COMMENT '设备ID',
    buyer_id      bigint COMMENT '设备对应用户ID',
    start_up_date date   COMMENT '设备启动日期',
    app_version   string COMMENT '设备安装app版本',
    platform      string COMMENT '设备平台',
    language_code string COMMENT '设备所选语言',
    region_code   string COMMENT 'geo_country',
    app_region_code   string COMMENT 'country',
    min_collector_time  TIMESTAMP COMMENT '当日登录最小时间',
    max_collector_time  TIMESTAMP COMMENT '当日登录最大时间'
) COMMENT '设备启动事实表'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;
