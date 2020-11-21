CREATE TABLE IF NOT EXISTS dwb.dwb_fd_app_user_coupon_order
(
    project_name     string comment '组织',
    platform_type    string COMMENT '平台',
    country_code 	 string COMMENT '国家',
    coupon_config_id      string COMMENT '优惠券配置ID',
    coupon_give           string COMMENT '红包发放量',
    coupon_used           string COMMENT '红包使用量',
    coupon_used_success   string COMMENT '红包使用成功量',
    coupon_used_1h        string COMMENT '获取红包1h内使用量',
    coupon_used_24h       string comment '获取红包1h-24h内使用量',
    coupon_used_48h       string COMMENT '获取红包24h-48h内使用量',
    coupon_used_72h       string COMMENT '获取红包48h-72h内使用量',
    coupon_used_greater_72h  string COMMENT '获取红包大于72h内使用量'
) COMMENT 'appp用户优惠券使用指标报表'
PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS ORC
TBLPROPERTIES ("orc.compress"="SNAPPY");


CREATE TABLE IF NOT EXISTS dwb.dwb_fd_app_user_coupon_order_rpt
(
    project_name     string comment '组织',
    country_code 	 string COMMENT '国家',
    coupon_config_id      string COMMENT '优惠券配置ID',
    coupon_give_cnt           bigint COMMENT '红包发放量',
    coupon_used_cnt           bigint COMMENT '红包使用量',
    coupon_used_success_cnt   bigint COMMENT '红包使用成功量',
    coupon_used_1h_cnt        bigint COMMENT '获取红包1h内使用量',
    coupon_used_24h_cnt       bigint comment '获取红包1h-24h内使用量',
    coupon_used_48h_cnt       bigint COMMENT '获取红包24h-48h内使用量',
    coupon_used_72h_cnt       bigint COMMENT '获取红包48h-72h内使用量',
    coupon_used_greater_72h_cnt  bigint COMMENT '获取红包大于72h内使用量'
) COMMENT 'appp用户优惠券使用指标报表'
PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS ORC
TBLPROPERTIES ("orc.compress"="SNAPPY");