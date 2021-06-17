DROP TABLE IF EXISTS mlb.mlb_vova_category_feat_d;
CREATE EXTERNAL TABLE IF NOT EXISTS mlb.mlb_vova_category_feat_d
(
    feat       STRING,
    weight     BIGINT
) COMMENT '深度学习模型特征' PARTITIONED BY (pt STRING,feat_type STRING)
    row format delimited fields terminated by '\001'  STORED AS PARQUETFILE
    location 's3://vova-mlb/REC/data/base/categroy_feature/mlb_vova_category_feat_d'
;