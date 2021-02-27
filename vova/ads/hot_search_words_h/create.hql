drop table ads.ads_vova_hot_search_word;
CREATE external TABLE IF NOT EXISTS ads.ads_vova_hot_search_word
(
    app_from                  string COMMENT 'ac|vova',
    language_id               bigint COMMENT '语言id',
    gender                    bigint COMMENT '性别,1:男性 2:女性 0:未知',
    is_shield                 bigint COMMENT '是否brand屏蔽 1:是 0:否',
    hot_word                  string COMMENT '热搜词',
    search_counts             bigint COMMENT '搜索次数'
) COMMENT '热搜词'
PARTITIONED BY ( pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;