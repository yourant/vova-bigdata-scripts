create external TABLE mlb.mlb_vova_rec_m_order_u2i_nb_d
(
buyer_id                bigint      COMMENT '用户id',
rec_goods_id_list       string      COMMENT '推荐商品列表',
score_list              string      COMMENT '推荐商品分数列表'
) COMMENT '订单行为u2i召回结果表' PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION "s3://vova-mlb/REC/data/match/match_result/order_u2i/no_brand_serial"
;


create external TABLE mlb.mlb_vova_rec_m_order_i2i_nb_d
(
goods_id                bigint      COMMENT '商品id',
rec_goods_id_list       string      COMMENT '推荐商品列表',
score_list              string      COMMENT '推荐商品分数列表'
) COMMENT '订单行为i2i召回结果表' PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION "s3://vova-mlb/REC/data/match/match_result/order_i2i/no_brand_serial"
;