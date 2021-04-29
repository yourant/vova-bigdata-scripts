
CREATE EXTERNAL TABLE mlb.mlb_vova_highfreq_query_mapping_d(
source_origin string COMMENT '原始query',
target_query string COMMENT '归并映射mapping之后的query'
) PARTITIONED BY(pt string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION "s3://vova-mlb/REC/data/match/match_result/mlb_vova_highfreq_query_mapping_d";


CREATE EXTERNAL TABLE mlb.mlb_vova_highfreq_query_match_d(
query_keys string COMMENT '归并映射后的query 与性别的组合，例如：query 为 nike, 性别为male 则query_key 为nike@male',
goods_list string COMMENT '序列化的商品列表，做大出500条，不足的从根据翻译后query从语义或者ES进行补充'
) PARTITIONED BY(pt string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION "s3://vova-mlb/REC/data/match/match_result/mlb_vova_highfreq_query_match_d";