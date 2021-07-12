drop table if exists dwb.dwb_vova_hot_search_words_no_brand;
CREATE EXTERNAL TABLE IF NOT EXISTS dwb.dwb_vova_hot_search_words_no_brand
(
    words            string COMMENT 'words',
    search_num       bigint COMMENT 'search_num',
    increment        string COMMENT 'increment'
) COMMENT '热搜词去除brand词后定期传给后台'
    PARTITIONED BY (pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;