个性化列表u2i召回
create external TABLE mlb.mlb_vova_rec_u2i_match_list_h
(
    buyer_id                bigint      COMMENT '用户id',
    cat_id                  bigint      COMMENT '品类id',
    rec_goods_list          string      COMMENT '推荐商品序列化结果',
    score_list              string      COMMENT '推荐结果得分序列化结果'
) COMMENT '列表页个性化召回结果表' PARTITIONED BY (pt STRING, hour STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS parquet
LOCATION "s3://vova-mlb/REC/data/match/match_result/mlb_vova_rec_u2i_match_list_h"

