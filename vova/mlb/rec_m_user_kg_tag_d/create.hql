[9149]列表页增加用户行为实时反馈&用户属性类目偏好-导数
https://zt.gitvv.com/index.php?m=task&f=view&taskID=33897

https://confluence.gitvv.com/pages/viewpage.action?pageId=21273310

create external TABLE mlb.mlb_vova_rec_m_user_cat_expand_d
(
    buyer_id                bigint      COMMENT '用户id',
    subcategory_list       string      COMMENT '子品类列表'
) COMMENT '用户对子品类偏好召回结果表' PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS parquet
LOCATION "s3://vova-mlb/REC/data/match/match_result/mlb_vova_rec_m_user_cat_expand_d";
