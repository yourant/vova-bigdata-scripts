-- ac 品牌会场
drop table if exists ads.ads_vova_activity_ac_brand;
create external table if  not exists  ads.ads_vova_activity_ac_brand (
    `goods_id`                  bigint COMMENT 'i_商品ID',
    `region_id`                 int    COMMENT 'i_国家id',
    `biz_type`                  STRING COMMENT 'i_biz type',
    `rp_type`                   int    COMMENT 'i_rp type',
    `first_cat_id`              int    COMMENT 'd_一级品类id',
    `second_cat_id`             int    COMMENT 'd_二级品类id',
    `rank`                      bigint COMMENT 'd_排名'
)COMMENT 'women cloth 活动表' PARTITIONED BY (pt string)
     STORED AS PARQUETFILE;


create external table if  not exists  ads.ads_vova_dictionary_act_brand_v2(
brand_id int  COMMENT 'brand id',
brand_name_desc string  COMMENT 'brand 名称简介'
);
