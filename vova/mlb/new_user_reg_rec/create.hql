[7832]冷启动兜底策略取数
https://zt.gitvv.com/index.php?m=task&f=view&taskID=29350
需求文档：

https://confluence.gitvv.com/pages/viewpage.action?pageId=6124960

https://docs.google.com/spreadsheets/d/1Rtd-HgODpyEmJYbtKZc2d3M1i_hCtNw1Of-fCoBPHPg/edit#gid=0
建表语句：
取数表2: 取近2周内活动用户的如下统计信息，根据用户国家region_id下聚合信息字段值。

# ads.ads_new_user_reg_rec
drop table mlb.mlb_vova_rec_new_user_reg_d;
create external TABLE mlb.mlb_vova_rec_new_user_reg_d
(
    region_id                string         COMMENT '国家/地区ID',
    cat_id                   bigint         COMMENT '商品类目id',
    goods_id                 bigint         COMMENT '商品id',
    clk_cnt                  bigint         COMMENT '点击量',
    expre_cnt                bigint         COMMENT '曝光量',
    impression_uv            bigint         COMMENT '曝光UV',
    gmv                      decimal(14,4)  COMMENT 'GMV',
    gcr                      decimal(14,4)  COMMENT 'GCR',
    sales_vol                bigint         COMMENT '销量',
    order_goods_cnt          bigint         COMMENT '订单量',
    refund_order_goods_cnt   bigint         COMMENT '退款订单量',
    collect_cnt              bigint         COMMENT '收藏数量',
    add_cat_cnt              bigint         COMMENT '加购次数',
    refund_rate              decimal(14,4)  COMMENT '退款率',
    comment_cnt              bigint         COMMENT '总评量',
    good_comment_cnt         bigint         COMMENT '好评量',
    bad_comment_cnt          bigint         COMMENT '差评量',
    rank_index               bigint         COMMENT 'GCR倒排索引（取top3000）',
    click_uv                 bigint         COMMENT '点击UV'

) COMMENT '用户冷启动兜底策略取数' PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION "s3://vova-mlb/REC/data/match/new_user/pull_analysis_data_region"
STORED AS textfile;


@@@@@@@@@@@@@
2021-01-18 结果数据表
--不区分brand产品召回结果
create external TABLE mlb.mlb_vova_rec_m_nurecallad_d
(
    cluster_key  string         COMMENT '分组标识',
    goods_id     bigint         COMMENT '商品id',
    rank_num     bigint         COMMENT '综合排序topN的rank索引'
) COMMENT '用户冷启动聚合数据' PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION "s3://vova-mlb/REC/data/match/new_user/recall_result_data/rec_m_nurecallad_d"
STORED AS textfile;

--非brand产品产召回结果
create external TABLE mlb.mlb_vova_rec_m_nurecallad_nb_d
(
    cluster_key  string         COMMENT '分组标识',
    goods_id     bigint         COMMENT '商品id',
    rank_num     bigint         COMMENT '综合排序topN的rank索引'
) COMMENT '用户冷启动聚合数据(no_brand)' PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION "s3://vova-mlb/REC/data/match/new_user/recall_result_data/rec_m_nurecallad_nb_d"
STORED AS textfile;

--不区分brand产品召回结果
create external TABLE mlb.mlb_vova_rec_m_nurecallreg_d
(
    region_id    bigint         COMMENT '分组标识',
    goods_id     bigint         COMMENT '商品id',
    rank_num     bigint         COMMENT '综合排序topN的rank索引'
) COMMENT '冷启动兜底策略' PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION "s3://vova-mlb/REC/data/match/new_user/recall_result_data_region/rec_m_nurecallreg_d"
STORED AS textfile;

--非brand产品产召回结果
create external TABLE mlb.mlb_vova_rec_m_nurecallreg_nb_d
(
    region_id    bigint         COMMENT '分组标识',
    goods_id     bigint         COMMENT '商品id',
    rank_num     bigint         COMMENT '综合排序topN的rank索引'
) COMMENT '冷启动兜底策略(no_brand)' PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION "s3://vova-mlb/REC/data/match/new_user/recall_result_data_region/rec_m_nurecallreg_nb_d"
STORED AS textfile;

####################################
导入mysql:

ads_rec_m_nurecallad_d
create table rec_recall.ads_rec_m_nurecallad_d (
    `id`           int(11)       NOT NULL AUTO_INCREMENT COMMENT '自增主键',
    `cluster_key`  varchar(100)  NOT NULL COMMENT '分组标识',
    `goods_id`     int(11)       NOT NULL COMMENT '商品id',
    `rank_num`     int(11)       NOT NULL COMMENT '综合排序topN的rank索引',
    PRIMARY KEY (`id`) USING BTREE,
    KEY cluster_key (cluster_key) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC COMMENT='用户冷启动聚合数据';

ads_rec_m_nurecallad_nb_d
create table rec_recall.ads_rec_m_nurecallad_nb_d (
    `id`           int(11)       NOT NULL AUTO_INCREMENT COMMENT '自增主键',
    `cluster_key`  varchar(100)  NOT NULL COMMENT '分组标识',
    `goods_id`     int(11)       NOT NULL COMMENT '商品id',
    `rank_num`     int(11)       NOT NULL COMMENT '综合排序topN的rank索引',
    PRIMARY KEY (`id`) USING BTREE,
    KEY cluster_key (cluster_key) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC COMMENT='用户冷启动聚合数据(非brand)';

ads_rec_m_nurecallreg_d
create table rec_recall.ads_rec_m_nurecallreg_d (
    `id`           int(11)       NOT NULL AUTO_INCREMENT COMMENT '自增主键',
    `region_id`    varchar(100)  NOT NULL COMMENT '分组标识',
    `goods_id`     int(11)       NOT NULL COMMENT '商品id',
    `rank_num`     int(11)       NOT NULL COMMENT '综合排序topN的rank索引',
    PRIMARY KEY (`id`) USING BTREE,
    KEY region_id (region_id) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC COMMENT='冷启动兜底策略';

ads_rec_m_nurecallreg_nb_d
create table rec_recall.ads_rec_m_nurecallreg_nb_d (
    `id`           int(11)       NOT NULL AUTO_INCREMENT COMMENT '自增主键',
    `region_id`    varchar(100)  NOT NULL COMMENT '分组标识',
    `goods_id`     int(11)       NOT NULL COMMENT '商品id',
    `rank_num`     int(11)       NOT NULL COMMENT '综合排序topN的rank索引',
    PRIMARY KEY (`id`) USING BTREE,
    KEY region_id (region_id) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC COMMENT='冷启动兜底策略(非brand)';
