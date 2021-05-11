create external TABLE mlb.mlb_search_spell_check
(
    cur_time string,
    device_id string,
    language_id string,
    query string,
    region_id string,
    spell_check_query string,
    translated_query string,
    uid bigint
) COMMENT '搜索纠错服务端打点日志'  PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    STORED AS PARQUETFILE
    location 's3://vova-mlb/REC/data/search/correct/mlb_search_spell_check';



