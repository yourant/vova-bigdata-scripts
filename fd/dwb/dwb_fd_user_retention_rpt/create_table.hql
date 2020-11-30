CREATE table if not exists dwb.dwb_fd_user_retention_rpt
(
    prc_date                string comment '当前北京时间',
    platform_type           string comment '平台',
    country                 string comment '国家',

    goods_amount            decimal(15, 4) comment '销售额',
    paid_orders             bigint  comment '已支付订单量',
    paid_users              bigint  comment'今天访问用户中有历史下单的',
    uv                       bigint comment 'uv(今天访问用户)',
    access_today_users_new   bigint COMMENT '今天新访问用户',

    access_1ago_users       bigint COMMENT '1天前访问用户',
    access_both1_users      bigint COMMENT '1天前和今天都访问用户',
    access_7ago_users       bigint COMMENT '7天前访问用户',
    access_both7_users      bigint COMMENT '7天前和今天都访问用户',
    access_28ago_users      bigint COMMENT '28天前访问用户',
    access_both28_users     bigint COMMENT '28天前和今天都访问用户',
    access_1ago_users_new   bigint COMMENT '1天前新访问用户',
    access_both1_users_new  bigint COMMENT '1天前新访问用户在今天也访问的用户',
    access_7ago_users_new   bigint COMMENT '7天前新访问用户',
    access_both7_users_new  bigint COMMENT '7天前新访问用户在今天也访问的用户',
    access_28ago_users_new  bigint COMMENT '28天前新访问用户',
    access_both28_users_new bigint COMMENT '28天前新访问用户在今天也访问的用户',
    uv_past_paid            bigint COMMENT '今天新访问用户中曾经下过单的用户'
) comment '各维度下的uv，订单量，销售额，用户n天留存分析'
    partitioned by (pt string)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
    STORED AS ORC
    TBLPROPERTIES ("orc.compress"="SNAPPY");