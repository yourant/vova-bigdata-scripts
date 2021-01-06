drop table dwd.dwd_vova_fact_web_start_up;
CREATE TABLE IF NOT EXISTS dwd.dwd_vova_fact_web_start_up
(
    datasource     string comment '数据平台',
    domain_userid  string COMMENT '设备ID',
    buyer_id      bigint COMMENT '设备对应用户ID',
    region_code   string COMMENT 'geo_country',
    first_page_url   string COMMENT 'page_url',
    first_referrer   string COMMENT 'referrer',
    min_create_time  TIMESTAMP COMMENT '当日登录最小时间',
    max_create_time  TIMESTAMP COMMENT '当日登录最大时间'
) COMMENT 'web用户访问表' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;


drop table dim.dim_vova_web_domain_userid;
CREATE TABLE IF NOT EXISTS dim.dim_vova_web_domain_userid
(
    domain_userid  string COMMENT 'domain_userid',
    buyer_id       bigint COMMENT '设备对应用户ID',
    activate_time  TIMESTAMP COMMENT '激活时间',
    first_order_id bigint COMMENT 'first_order_id',
    first_pay_time TIMESTAMP COMMENT 'first_pay_time',
    medium         string COMMENT 'medium',
    source         string COMMENT 'source',
    reg_time         TIMESTAMP COMMENT '用户注册时间',
    register_success_time         TIMESTAMP COMMENT '打点数据点击注册按钮返回成功时间'
) COMMENT 'web domain_userid作为unique key'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;


drop table tmp.tmp_vova_web_main_process_register;
CREATE TABLE tmp.tmp_vova_web_main_process_register
(
    datasource      STRING COMMENT '数据平台',
    domain_userid   STRING COMMENT '',
    min_create_time TIMESTAMP COMMENT '当日登录最小时间'
) COMMENT '点击注册按钮返回成功'
  PARTITIONED BY (pt STRING);


