-- 用户画像二级品类加关键词评分统计表
drop table if exists ads.ads_buyer_portrait_second_category_word_score;
create table if  not exists  ads.ads_buyer_portrait_second_category_word_score (
    `buyer_id`                    int    COMMENT 'd_买家id',
    `second_cat_id`               int    COMMENT 'd_品类id',
    `word`                        STRING COMMENT 'i_商品关键词',
    `score`                       decimal(13,2) COMMENT 'i_评分'
)PARTITIONED BY (pt string)   COMMENT '用户画像二级品类加关键词评分统计表'
     STORED AS PARQUETFILE;
