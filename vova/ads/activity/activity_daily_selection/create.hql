-- 运营精选活动数据

drop table if exists ads.ads_vova_activity_daily_selection;
create external table if  not exists  ads.ads_vova_activity_daily_selection (
    `biz_type`                  string COMMENT 'i_biz_type',
    `event_type`                int    COMMENT 'i_类型',
    `region_id`                 bigint COMMENT 'i_国家id',
    `first_cat_id`              bigint COMMENT 'i_一级品类id',
    `goods_id`                  bigint COMMENT 'd_商品id',
    `rank`                      bigint COMMENT 'd_排名'
)COMMENT 'deaily se_v2数据' PARTITIONED BY (pt STRING)
     STORED AS PARQUETFILE;

# 会场逻辑统一
create table if not exists ads.ads_vova_activity_daily_selection_v2 (
goods_id                  bigint COMMENT 'i_商品ID',
region_id                 int    COMMENT 'i_国家id',
biz_type                  STRING COMMENT 'i_biz type',
rp_type                   int    COMMENT 'i_rp type 标记位',
first_cat_id              int    COMMENT 'd_一级品类id',
second_cat_id             int    COMMENT 'd_二级品类id',
rank                      bigint COMMENT 'd_排名'
)COMMENT 'deaily se数据v2' PARTITIONED BY (pt string)
     STORED AS PARQUETFILE;
