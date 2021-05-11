DROP TABLE ads.ads_vova_sale_goods_m;
CREATE EXTERNAL TABLE IF NOT EXISTS ads.ads_vova_sale_goods_m
(
    goods_id             bigint,
    sales_order          bigint COMMENT '销量'
) COMMENT 'ads_vova_sale_goods_m' PARTITIONED BY (pt STRING)
    STORED AS PARQUETFILE;

DROP TABLE ads.ads_vova_sale_goods_3m;
CREATE EXTERNAL TABLE IF NOT EXISTS ads.ads_vova_sale_goods_3m
(
    goods_id             bigint,
    sales_order          bigint COMMENT '销量'
) COMMENT 'ads_vova_sale_goods_3m' PARTITIONED BY (pt STRING)
    STORED AS PARQUETFILE;

select pt,count(*),count(distinct goods_id) from ads.ads_vova_sale_goods_m group by pt;
select pt,count(*),count(distinct goods_id) from ads.ads_vova_sale_goods_m where sales_order > 0 group by pt;

