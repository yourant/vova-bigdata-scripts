drop table mlb.mlb_vova_search_correct_gram_d;
create table mlb.mlb_vova_search_correct_gram_d
(
   src_word   string  comment '前词',
   dist_word  string  comment '后词',
   molecular  bigint  comment '概率分子',
   denominator bigint comment '概率分母',
   prob       double  comment '概率'
) comment '搜索词'
PARTITIONED BY(pt string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   -- 优先\t, 其次逗号(,), 若存储格式为parquet，则用\001
STORED AS parquet
LOCATION "s3://vova-mlb/REC/data/search/correct/mlb_vova_search_correct_gram_d"
;