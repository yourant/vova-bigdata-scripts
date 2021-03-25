drop table dwb.dwb_vova_mct_shield;
CREATE external  TABLE IF NOT EXISTS dwb.dwb_vova_mct_shield
(
    datasource     string comment '数据平台',
    event_date                     date   COMMENT '订单确认日期',
    mct_id                         bigint COMMENT '商品卖家ID',
    first_cat_id                   bigint COMMENT '一级分类ID',
    first_cat_name                 string COMMENT '一级分类名字',
    normal_order_3to6              bigint COMMENT '3-6周订单数',
    threshold_normal_order_5to8    bigint,
    normal_order_5to8              bigint COMMENT '5-8周订单数',
    normal_order_9to12             bigint COMMENT '9-12周订单数',
    valid_order_3to6               bigint COMMENT '7天有效发货3-6周',
    delivered_order_5to8           bigint COMMENT '交期内物流妥投5-8周',
    logistic_refund_order_9to12    bigint COMMENT '物流退款订9-12周',
    not_logistic_refund_order_5to8 bigint COMMENT '非物流退款订单5-8周'
) COMMENT '商家屏蔽' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;
    
    
CREATE external TABLE dwb.dwb_vova_goods_sn_1d(
  goods_sn string COMMENT 'goods_sn', 
  pv_goods_impression_1w bigint COMMENT 'goods_impression pv', 
  pv_goods_click_1w bigint COMMENT 'goods_click pv', 
  uv_1w bigint COMMENT '??UV', 
  gmv_1w decimal(13,4) COMMENT '??', 
  gcr_1w decimal(10,4) COMMENT 'gcr', 
  ctr_1w decimal(10,4) COMMENT 'ctr', 
  create_time timestamp COMMENT '', 
  last_update_time timestamp COMMENT '')
PARTITIONED BY (pt string)  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

CREATE external TABLE dwb.dwb_vova_goods_id_1d(
  goods_id int COMMENT 'goods_id',
  refund_rate_nonlogistics_8w decimal(10,4) COMMENT '')
PARTITIONED BY (pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

