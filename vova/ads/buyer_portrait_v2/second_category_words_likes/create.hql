-- 用户画像二级品类加关键词评分统计表
drop table if exists ads.ads_vova_buyer_portrait_second_category_word_score;
create external table if  not exists  ads.ads_vova_buyer_portrait_second_category_word_score (
    `buyer_id`                    int    COMMENT 'd_买家id',
    `second_cat_id`               int    COMMENT 'd_品类id',
    `word`                        STRING COMMENT 'i_商品关键词',
    `score`                       decimal(13,2) COMMENT 'i_评分'
)COMMENT '用户画像二级品类加关键词评分统计表' PARTITIONED BY (pt string)
     STORED AS PARQUETFILE;



CREATE external TABLE `dwd.dwd_vova_fact_goods_key_words`(
  `goods_id` int,
  `goods_name` string,
  `gender` string,
  `style` string,
  `season` string,
  `color` string,
  `model_number` string,
  `key_words` string,
  `last_update_time` string)
  stored as parquetfile;

