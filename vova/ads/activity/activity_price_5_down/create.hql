drop table if exists ads.ads_vova_activity_price_5_down;
create table if not exists ads.ads_vova_activity_price_5_down
(
    goods_id      bigint COMMENT 'i_商品ID',
    region_id     int COMMENT 'i_国家id',
    biz_type      STRING COMMENT 'i_biz type',
    rp_type       int COMMENT 'i_rp type',
    first_cat_id  int COMMENT 'd_一级品类id',
    second_cat_id int COMMENT 'd_二级品类id',
    rank          bigint COMMENT 'd_排名'
) COMMENT '#10065 新增货品池' PARTITIONED BY (pt string)
    STORED AS PARQUETFILE;

drop table if exists ads.ads_vova_activity_price_5_down_goods;
create table if not exists ads.ads_vova_activity_price_5_down_goods
(
    goods_id      bigint COMMENT 'i_商品ID',
    region_id     int COMMENT 'i_国家id',
    biz_type      STRING COMMENT 'i_biz type',
    rp_type       int COMMENT 'i_rp type',
    first_cat_id  int COMMENT 'd_一级品类id',
    second_cat_id int COMMENT 'd_二级品类id',
    rank          bigint COMMENT 'd_排名'
) COMMENT '#10065 新增货品池(新人会场)' PARTITIONED BY (pt string)
    STORED AS PARQUETFILE;