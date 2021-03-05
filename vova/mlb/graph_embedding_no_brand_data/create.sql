DROP TABLE IF EXISTS mlb.mlb_vova_graph_embedding_no_brand_data;
CREATE EXTERNAL TABLE IF NOT EXISTS mlb.mlb_vova_graph_embedding_no_brand_data
(
    id1   bigint,
    id2   bigint,
    score DOUBLE
) COMMENT 'graph_embedding_no_brand_data' PARTITIONED BY (pt STRING)
    row format delimited fields terminated by ','
    location 's3://vova-mlb/REC/data/match/match_result/graph_embedding/no_brand'