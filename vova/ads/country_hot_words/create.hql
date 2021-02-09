drop table ads.ads_vova_country_hot_search_words;
CREATE external TABLE IF NOT EXISTS ads.ads_vova_country_hot_search_words
(
    region_id                 bigint COMMENT '国家id',
    region_code               string COMMENT '国家代码',
    hot_words                 string COMMENT '热搜词',
    rank                      bigint COMMENT '排名'
) COMMENT '国家topN热搜词'
PARTITIONED BY ( pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;


drop table tmp.ads_country_hot_search_words;
CREATE TABLE IF NOT EXISTS  tmp.ads_country_hot_search_words
(
region_code string COMMENT '国家代码',
hot_words string COMMENT '热搜词',
cnt bigint COMMENT '数量'
) COMMENT '国家topN热搜词'
 ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;