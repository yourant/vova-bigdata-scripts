-- 用户查询表
CREATE external TABLE ads.ads_vova_user_analysis(
  `order_id`         bigint COMMENT 'i_父订单id',
  `order_goods_id`   bigint COMMENT 'i_子订单id',
  `buyer_id`         bigint COMMENT 'd_买家id',
  `device_id`        string COMMENT 'd_设备id',
  `goods_number`     bigint COMMENT 'd_商品数',
  `shop_price`       decimal(13,2)  COMMENT 'd_价格',
  `shipping_fee`     decimal(13,2)  COMMENT 'd_运费',
  `gender`           string         COMMENT 'd_性别',
  `user_age_group`   string         COMMENT 'd_年龄区间',
  `main_channel`     string         COMMENT 'd_渠道来源'
)COMMENT '用户查询表' PARTITIONED BY (pt STRING)   stored as parquetfile;