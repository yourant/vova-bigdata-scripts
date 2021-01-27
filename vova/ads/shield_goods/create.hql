--商品维度汇总表
drop table ads.ads_shield_goods_detail;
CREATE TABLE IF NOT EXISTS ads.ads_shield_goods_detail
(
    buyer_id           BIGINT COMMENT '用户id',
    device_id          String COMMENT '设备号',
    goods_id_list     String COMMENT '屏蔽商品id'
) COMMENT '商品屏蔽'
 PARTITIONED BY ( pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;


drop table ads.ads_vova_shield_goods;
CREATE external TABLE IF NOT EXISTS ads.ads_vova_shield_goods
(
    goods_id           BIGINT COMMENT '商品id',
    shield_cnt         BIGINT COMMENT '屏蔽次数'
) COMMENT '商品屏蔽'
 PARTITIONED BY ( pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;