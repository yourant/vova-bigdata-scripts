drop table dwd.dwd.vova_fact_rec_search_log;
CREATE TABLE IF NOT EXISTS dwd.vova_fact_rec_search_log
(
    event_date    string COMMENT '日期',
    query         string COMMENT '搜索词',
    goods_cnt     bigint COMMENT '搜索商品次数',
    buyer_id      bigint COMMENT '用户id'
) COMMENT 'es搜索日志表'
 PARTITIONED BY ( pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;