drop table tmp.tmp_vova_buyer_first_pay;
CREATE TABLE IF NOT EXISTS tmp.tmp_vova_buyer_first_pay
(
    datasource       string comment '数据平台',
    buyer_id         bigint COMMENT '买家ID',
    first_order_id   bigint COMMENT '买家支付首单ID',
    first_order_time timestamp COMMENT '买家支付首单下单时间',
    first_pay_time   timestamp
) COMMENT '买家首单信息'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;



drop table tmp.tmp_vova_buyer_first_refund;
CREATE TABLE IF NOT EXISTS tmp.tmp_vova_buyer_first_refund
(
    datasource       string comment '数据平台',
    buyer_id         bigint COMMENT '买家ID',
    first_refund_time timestamp COMMENT '买家首单退款时间'
) COMMENT '买家退款首单信息'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;



drop table dim.dim_vova_buyers;
CREATE TABLE IF NOT EXISTS dim.dim_vova_buyers
(
    datasource       string comment '数据平台',
    buyer_id         bigint COMMENT '买家ID',
    email            string COMMENT '邮箱',
    buyer_name       string COMMENT '买家名字',
    gender           string COMMENT '买家性别',
    birthday         timestamp COMMENT '买家生日',
    reg_time         string COMMENT '买家注册时间',
    platform         string COMMENT '设备平台(android, ios, pc, mob, unknown)',
    reg_page         string COMMENT '买家注册页面',
    region_id        bigint COMMENT '买家注册国家ID',
    region_code      string COMMENT '买家注册国家缩写',
    language_id      bigint COMMENT '买家注册语言ID',
    language_code    string COMMENT '买家注册语言缩写',
    reg_method       string COMMENT '买家注册方式google,facebook,vova',
    reg_site_host    string COMMENT '买家注册站点',
    first_order_id   bigint COMMENT '买家支付首单ID',
    first_order_time timestamp COMMENT '买家支付首单下单时间',
    first_pay_time   timestamp,
    first_refund_time timestamp COMMENT '买家首单退款时间',
    user_age_group STRING COMMENT 'user_age_group',
     current_device_id STRING COMMENT 'current_device_id',
     current_app_version STRING COMMENT 'current_app_version',
     last_start_up_date DATE, bind_time TIMESTAMP COMMENT '邮箱绑定日期'
) COMMENT '买家维度表'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;



