drop table if exists ads.ads_vova_buyer_page_tag;
CREATE external TABLE `ads`.`ads_vova_buyer_page_tag`(
`buyer_id`                   BIGINT        COMMENT 'd_买家id',
`datasource`                 STRING        COMMENT 'i_datasource',
`device_id`                  STRING        COMMENT 'i_用户设备号',
`email`                      STRING        COMMENT 'i_用户邮箱',
`reg_gender`                 STRING        COMMENT 'i_注册性别',
`reg_age_group`              STRING        COMMENT 'i_注册年龄段',
`reg_time`                   DATE          COMMENT 'i_注册时间',
`reg_ctry`                   STRING        COMMENT 'i_注册国家',
`lag_id`                     STRING        COMMENT 'i_语言ID',
`reg_channel`                STRING        COMMENT 'i_注册渠道',
`os_type`                    STRING        COMMENT 'i_手机系统',
`first_order_time`           DATE          COMMENT 'i_首单时间',
`last_order_time`            DATE          COMMENT 'i_最近一单时间',
`order_cnt`                  INT           COMMENT 'i_子订单量',
`avg_price`                  DECIMAL(13,2) COMMENT 'i_订单平均价格',
`price_range_type`           INT           COMMENT 'i_价格偏好层级',
`buy_times_type`             INT           COMMENT 'i_消费频率',
`reg_tag`                    STRING        COMMENT 'i_注册时长',
`buyer_act`                  STRING        COMMENT 'i_用户活跃度',
`trade_act`                  STRING        COMMENT 'i_交易阶段',
`first_cat_likes`            STRING        COMMENT 'i_一级品类偏好',
`second_cat_likes`           STRING        COMMENT 'i_二级品类偏好',
`second_cat_style_likes`     STRING        COMMENT 'i_二级品类+风格偏好',
`second_cat_key_word_likes`  STRING        COMMENT 'i_二级品类+关键词偏好',
`second_cat_price_likes`     STRING        COMMENT 'i_用户二级品类和价格偏好',
`brand_likes`                STRING        COMMENT 'i_品牌偏好',
`searchs`                    STRING        COMMENT 'i_用户搜索词'
)
COMMENT '页面展示用户标签表'
PARTITIONED BY (`bpt` STRING);

