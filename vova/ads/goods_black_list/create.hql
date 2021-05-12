drop table ads.ads_vova_goods_black_list;
CREATE external TABLE IF NOT EXISTS ads.ads_vova_goods_black_list
(
    goods_id         BIGINT COMMENT '商品ID'
) COMMENT '商品黑名单'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

drop table ads.ads_vova_goods_black_list_arc;
CREATE external TABLE IF NOT EXISTS ads.ads_vova_goods_black_list_arc
(
    goods_id         BIGINT COMMENT '商品ID'
) COMMENT '商品黑名单'
PARTITIONED BY ( pt string)  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

