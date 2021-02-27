DROP TABLE ads.ads_vova_goods_display_sort;
CREATE EXTERNAL TABLE IF NOT EXISTS ads.ads_vova_goods_display_sort
(
    goods_id         bigint COMMENT 'goods_id',
    gender           string COMMENT '性别',
    platform         string COMMENT '平台(mob,web)',
    project_name     string COMMENT '项目',
    sales_order      bigint COMMENT '近7日销售订单数',
    gmv              decimal(15, 2) COMMENT 'gmv',
    impressions      bigint COMMENT '曝光量',
    clicks           bigint COMMENT '点击量',
    users            bigint COMMENT '点击uv',
    last_update_time TIMESTAMP COMMENT '最后更新时间'
) COMMENT '商品排序' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;


