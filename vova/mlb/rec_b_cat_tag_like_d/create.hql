[9166]首页新增用户类目偏好发现召回-导数
https://zt.gitvv.com/index.php?m=task&f=view&taskID=33894

confluence: https://confluence.gitvv.com/pages/viewpage.action?pageId=21273290

mlb.mlb_rec_m_catlike_d
create external TABLE mlb.mlb_rec_m_catlike_d
(
    region_id           bigint               COMMENT '区域',
    gender              string               COMMENT '性别',
    user_age_group      string               COMMENT '用户年龄组',
    first_cat_id         string              COMMENT '一级品类',
    second_cat_id        string              COMMENT '二级品类',
    goods_id        bigint         COMMENT '商品id',
    second_rank     bigint        COMMENT '商品得分',
    home_rank            bigint        COMMENT '首页按cluster_key排序值'
    first_rank           bigint              COMMENT '按一级品类排序值'
) COMMENT '类目偏好召回' PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '/001'
STORED AS parquet
LOCATION "s3://vova-mlb/REC/data/match/new_user/recall_result_data/rec_m_catlike_d";

依赖：mlb.mlb_vova_user_behave_link_d，mlb.mlb_vova_rec_b_goods_score_d 两个表


create external TABLE mlb.mlb_rec_m_taglike_d
(
    region_id           bigint               COMMENT '区域',
    gender              string               COMMENT '性别',
    user_age_group      string               COMMENT '用户年龄组',
    first_cat_id        string               COMMENT '一级品类',
    second_cat_id       string               COMMENT '二级品类',
    goods_id        bigint        COMMENT '商品id',
    goods_score         dobule        COMMENT '商品得分',
    home_rank           bigint        COMMENT '首页按cluster_key排序值'
    first_rank          bigint              COMMENT '按一级品类排序值'
) COMMENT '知识图谱偏好召回' PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '/0001'
LOCATION "s3://vova-mlb/REC/data/match/new_user/recall_result_data/rec_m_taglike_d"
STORED AS parquet;


create external TABLE mlb.mlb_rec_m_tagcombine_d
(
    region_id           bigint               COMMENT '区域',
    gender              string               COMMENT '性别',
    user_age_group      string               COMMENT '用户年龄组',
    kg_tag_combine   string               COMMENT '标签组合',
    tag_scoredobule          COMMENT '标签打分',
) COMMENT '知识图谱偏好召回' PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '/0001'
LOCATION "s3://vova-mlb/REC/data/match/new_user/recall_result_data/rec_m_taglike_d"
STORED AS parquet;