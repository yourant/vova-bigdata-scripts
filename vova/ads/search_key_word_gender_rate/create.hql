drop table ads.ads_vova_search_gender_rate;
CREATE external TABLE IF NOT EXISTS ads.ads_vova_search_gender_rate
(
    `key_word`       string comment 'i_搜索词',
    `male_search_count`     int    comment 'i_男性搜索次数',
    `female_search_count`   int    comment 'i_女性搜索次数'
) COMMENT 'fact_search_gender_rate' PARTITIONED BY (pt string)
     STORED AS PARQUETFILE;

