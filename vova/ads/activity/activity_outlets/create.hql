drop table if exists ads.ads_vova_activity_outlets;
create external table if  not exists  ads.ads_vova_activity_outlets (
    `biz_type`                  string COMMENT 'i_biz_type',
    `event_type`                bigint COMMENT 'i_类型',
    `region_id`                 bigint COMMENT 'i_国家id',
    `first_cat_id`              bigint COMMENT 'i_一级品类id',
    `second_cat_id`             bigint COMMENT 'i_二级品类id',
    `goods_id`                  bigint COMMENT 'd_商品id',
    `rank`                      bigint COMMENT 'd_排名'
) COMMENT 'outlets活动数据' PARTITIONED BY (pt STRING)
     STORED AS PARQUETFILE;

// 统一表结构 V2
create table if  not exists ads.ads_vova_activity_outlets_v2 (
goods_id                  bigint COMMENT 'i_商品ID',
region_id                 int    COMMENT 'i_国家id',
biz_type                  STRING COMMENT 'i_biz type',
rp_type                   int    COMMENT 'i_rp type 标记位',
first_cat_id              int    COMMENT 'd_一级品类id',
second_cat_id             int    COMMENT 'd_二级品类id',
rank                      bigint COMMENT 'd_排名'
)COMMENT 'outlets活动数据v2' PARTITIONED BY (pt string)
     STORED AS PARQUETFILE;

#【数据】会场逻辑统一&outlets规则调整
# 需求方想要数据量不变，只把改标记位改为3








