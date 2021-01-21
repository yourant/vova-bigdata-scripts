DROP TABLE dwb.dwb_zq_goods_cat_behave;
CREATE EXTERNAL TABLE IF NOT EXISTS dwb.dwb_zq_goods_cat_behave
(
    event_date      DATE COMMENT 'd_date',
    domain_group    string COMMENT 'd_站点所属组织',
    datasource      string COMMENT 'd_平台',
    original_source string COMMENT 'd_商品来源',
    first_cat_name  string COMMENT 'd_first_cat_name',
    second_cat_name string COMMENT 'd_second_cat_name',
    expres          bigint COMMENT '曝光数',
    clks            bigint COMMENT '点击数',
    cart_pv         bigint COMMENT '加购量',
    sale_cnt        bigint COMMENT '支付单数',
    gmv             decimal(15, 2) COMMENT 'gmv'
) COMMENT 'dwb_zq_goods_cat_behave' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

DROP TABLE dwb.dwb_zq_goods_sale_data;
CREATE EXTERNAL TABLE IF NOT EXISTS dwb.dwb_zq_goods_sale_data
(
    event_date      DATE COMMENT 'd_date',
    domain_group    string COMMENT 'd_站点所属组织',
    goods_id        bigint COMMENT 'd_goods_id',
    datasource      string COMMENT 'd_平台',
    original_source string COMMENT 'd_商品来源',
    first_cat_name  string COMMENT 'd_first_cat_name',
    second_cat_name string COMMENT 'd_second_cat_name',
    expres          bigint COMMENT '曝光数',
    clks            bigint COMMENT '点击数',
    cart_pv         bigint COMMENT '加购量',
    sale_cnt        bigint COMMENT '支付单数',
    gmv             decimal(15, 2) COMMENT 'gmv'
) COMMENT 'dwb_zq_goods_sale_data' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;