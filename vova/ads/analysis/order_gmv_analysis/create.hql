-- 订单量笔单价gmv分析表
CREATE external TABLE ads.ads_vova_order_gmv_analysis(
  `order_cnt`       int                   COMMENT '订单数',
  `avg_price`       decimal(13,2)         COMMENT '笔单价',
  `gmv`             decimal(13,2)         COMMENT 'gmv'
)COMMENT '订单量笔单价gmv分析表' PARTITIONED BY (pt STRING)   stored as parquetfile;