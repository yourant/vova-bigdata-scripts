drop table dwd.dwd_vova_fact_search_word;
CREATE EXTERNAL TABLE IF NOT EXISTS dwd.dwd_vova_fact_search_word
(
    `datasource`     string comment 'i_数据平台',
    `search_time`    timestamp comment 'i_搜索时间',
    `buyer_id`       bigint comment 'd_用户id',
    `key_word`       string comment 'i_搜索词'
) PARTITIONED BY (pt string)
     STORED AS PARQUETFILE;

