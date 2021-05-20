drop table if exists tmp.tmp_green_health_goods_id;
create
    external table if not exists tmp.tmp_green_health_goods_id
(
    `goods_id` bigint COMMENT 'd_商品id'
) COMMENT '在要求范围内的goods_id' PARTITIONED BY (pt STRING)
    STORED AS PARQUETFILE
;

drop table if exists ads.ads_vova_activity_green_house;
create table if  not exists ads.ads_vova_activity_green_house (
goods_id                  bigint COMMENT 'i_商品ID',
region_id                 int    COMMENT 'i_国家id',
biz_type                  STRING COMMENT 'i_biz type',
rp_type                   int    COMMENT 'i_rp type 标记位',
first_cat_id              int    COMMENT 'd_一级品类id',
second_cat_id             int    COMMENT 'd_二级品类id',
rank                      bigint COMMENT 'd_排名'
)COMMENT '绿茵活动数据' PARTITIONED BY (pt string)
    STORED AS PARQUETFILE;
;