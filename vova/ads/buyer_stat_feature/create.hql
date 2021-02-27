-- 用户属性标签
drop table if exists ads.ads_vova_buyer_stat_feature;
create external table if  not exists  ads.ads_vova_buyer_stat_feature (
    `buyer_id`                  int             COMMENT 'i_用户id',
    `reg_gender`                string          COMMENT 'd_注册性别',
    `reg_age_group`             string          COMMENT 'd_注册年龄',
    `reg_ctry`                  string          COMMENT 'd_注册国家',
    `reg_time`                  TIMESTAMP       COMMENT 'd_注册时间',
    `reg_channel`               string          COMMENT 'd_注册渠道',
    `os_type`                   string          COMMENT 'd_系统类型',
    `first_cat_likes`           string          COMMENT 'd_一级品类偏好Top3',
    `second_cat_likes`          string          COMMENT 'd_二级品类偏好Top3',
    `first_order_time`          TIMESTAMP       COMMENT 'd_首单时间',
    `last_order_time`           TIMESTAMP       COMMENT 'd_最近下单时间',
    `order_cnt`                 int             COMMENT 'd_购买订单数',
    `avg_price`                 decimal(13,2)   COMMENT 'd_笔单价',
    `price_range`               int             COMMENT 'd_价格偏好层级',
    `buyer_act`                 string          COMMENT 'd_活跃度',
    `trade_act`                 string          COMMENT 'd_交易阶段',
    `last_logint_type`          int             COMMENT 'd_上次登入时间',
    `last_buyer_type`           int             COMMENT 'd_上次购买时间',
    `buy_times_type`            int             COMMENT 'd_消费频率',
    `email_act`                 int             COMMENT 'd_EDM-邮件分组'
) COMMENT '用户属性标签'
     STORED AS PARQUETFILE;
