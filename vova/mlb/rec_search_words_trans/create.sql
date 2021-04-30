create external TABLE tmp.tmp_vova_search_words_d
(
    key_words string COMMENT '搜索词',
    language   string COMMENT '语言'
) COMMENT '每天的搜索词' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '^#^'
    LOCATION "s3://zz-tr-dev/project/rec-trans/data/"
;


create external TABLE tmp.tmp_vova_search_words_trans_result_json
(
    trans_result string
) COMMENT '翻译结果' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '^#^'
    LOCATION "s3://zz-tr-dev/project/rec-trans/trans_result/"
;

create external TABLE tmp.tmp_vova_search_words_trans_result
(
    source string,
    result string
) COMMENT '翻译结果'  PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '^#^'
    STORED AS PARQUETFILE;

create external TABLE tmp.tmp_vova_search_words_trans_result_800
(
    source string COMMENT '搜索词',
    result   string COMMENT '语言'
) COMMENT '手动修改的搜索词'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    LOCATION "s3://vomkt-emr-rec/hjt/search_words_trans_result/"
;

CREATE EXTERNAL TABLE mlb.mlb_vova_user_query_translation_d(

    clk_from string COMMENT '原始query',
    translation_query string COMMENT '翻译之后的结果'

)COMMENT '搜索词翻译' PARTITIONED BY (pt STRING)
STORED AS PARQUET
LOCATION "s3://vova-mlb/REC/data/base/mlb_vova_user_query_translation_d/"
TBLPROPERTIES ('parquet.compress'='SNAPPY');