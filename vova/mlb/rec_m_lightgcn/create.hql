[8466]增加lightGCN召回-导数
https://zt.gitvv.com/index.php?m=task&f=view&taskID=31794
https://confluence.gitvv.com/pages/viewpage.action?pageId=21269943

u2i:
非brand商品base64召回结果输出目录：s3://vova-mlb/REC/data/match/match_result/lightgcn/u2i/no_brand_serial/pt=yyyy-mm-dd

输出格式<buyer_id, rec_goods_id_list, score_list>，\t分隔
序列化方法为小端在前

drop table  mlb.mlb_vova_rec_m_lightgcn_u2i_nb_d;
create external TABLE mlb.mlb_vova_rec_m_lightgcn_u2i_nb_d
(
    buyer_id           bigint    COMMENT '用户id',
    rec_goods_id_list  string    COMMENT '推荐商品列表',
    score_list         string    COMMENT '推荐商品分数列表'
) COMMENT 'lightgcn_u2i 非brand召回结果表' PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION "s3://vova-mlb/REC/data/match/match_result/lightgcn/u2i/no_brand_serial/"
STORED AS textfile;

select count(*) from  mlb.mlb_vova_rec_m_lightgcn_u2i_nb_d where pt='2021-02-25' limit 20;
657190

i2i:
非brand商品base64召回结果输出目录：s3://vova-mlb/REC/data/match/match_result/lightgcn/i2i/no_brand_serial/pt=yyyy-mm-dd

输出格式<goods_id, rec_goods_id_list, score_list>，\t分隔
序列化方法为小端在前

create external TABLE mlb.mlb_vova_rec_m_lightgcn_i2i_nb_d
(
    goods_id                bigint      COMMENT '商品id',
    rec_goods_id_list       string      COMMENT '推荐商品列表',
    score_list              string      COMMENT '推荐商品分数列表'
) COMMENT 'lightgcn_i2i 非brand召回结果表' PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION "s3://vova-mlb/REC/data/match/match_result/lightgcn/i2i/no_brand_serial/"
STORED AS textfile;

select count(*) from  mlb.mlb_vova_rec_m_lightgcn_i2i_nb_d where pt='2021-02-25' limit 20;
1011571
