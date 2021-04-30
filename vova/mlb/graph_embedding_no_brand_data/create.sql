DROP TABLE IF EXISTS mlb.mlb_vova_graph_embedding_no_brand_data;
CREATE EXTERNAL TABLE IF NOT EXISTS mlb.mlb_vova_graph_embedding_no_brand_data
(
    goods_id   bigint,
    rec_goods_id_list   string,
    score_list string
) COMMENT 'graph_embedding_no_brand_data' PARTITIONED BY (pt STRING)
    row format delimited fields terminated by '\t'
    location 's3://vova-mlb/REC/data/match/match_result/graph_embedding/brand_serial'