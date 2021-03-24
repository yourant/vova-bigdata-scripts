DROP TABLE ads.ads_vova_sale_goods_m;
CREATE EXTERNAL TABLE IF NOT EXISTS ads.ads_vova_sale_goods_m
(
    goods_id             bigint
) COMMENT 'ads_vova_sale_goods_m' PARTITIONED BY (pt STRING)
    STORED AS PARQUETFILE;
select count(*),count(distinct goods_id) from ads.ads_vova_sale_goods_m;