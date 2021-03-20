[7381]基于用户属性的新用户冷启动策略
需求背景及目的
"新用户的首次推荐对于用户留存非常重要，由于vova app目前收集到的用户维度的信息较少，目前依然采用热榜的方法进行推荐。
本案尽可能的将用户的基础维度信息用于召回，期望能在原有的基础上能一定程度的提升新用户的物品召回效果。
因此需要预先提取部分数据进行数据探索，后续将基于该需求数据进行推荐。"

限定条件
日期（必填）	2020-12-08之后，数据日更
组织（必填）	vova
平台（必填）	所有平台
其他限定条件

字段说明
字段名	字段业务口径（定义、公式或逻辑）	备注
cluster_key	年龄段&国家&性别&终端类型（若对应字段为null则定为unknow）	例子18~30&SK&male&ios，unknow&unknow&unknow&unknow
	年龄段age_range(不存在为unknown)
	国家country(不存在为unknown)
	终端platform(不存在为unknown)
	性别gender(不存在为unknown)
	字段参考dws.dws_buyer_portrait
cat_id	商品类目id	1235466
good_id	商品id	1235466
clicks	该cluster_key下本商品的曝光量
impressions
impression_uv	该cluster_key下该物品曝光UV（未交互用户不参与计算）
gmv	该cluster_key下本商品的gmv
gcr	该cluster_key下本商品的gcr（未交互用户不参与计算）
sale_vol	该cluster_key下本商品的销量
refund_rate	该cluster_key下本商品的退款率
good_comment_cnt	该cluster_key下本商品的好评数
bad_comment_cnt	该cluster_key下本商品的差评数
rank_index	该cluster_key下根据gcr，gmv，销量降序排列的索引	每个cluster_key保留top3000商品
pt	分区	2020-12-03

drop table mlb.mlb_vova_rec_new_user_d;
create external TABLE mlb.mlb_vova_rec_new_user_d
(
  user_age_group         STRING          COMMENT '年龄段',
  region_code            STRING          COMMENT '国家/地区',
  platform               STRING          COMMENT '平台',
  gender                 STRING          COMMENT '性别',
  cat_id                 bigint          COMMENT '商品类目id',
  goods_id               bigint          COMMENT '商品id',
  clk_cnt                bigint          COMMENT '点击量',
  expre_cnt              bigint          COMMENT '曝光量',
  click_uv               bigint          COMMENT '点击UV',
  impression_uv          bigint          COMMENT '曝光UV',
  gmv                    decimal(14,4)   COMMENT 'GMV',
  gcr                    decimal(14,4)   COMMENT 'GCR',
  sales_vol              bigint          COMMENT '销量',
  order_goods_cnt        bigint          COMMENT '子订单量',
  refund_order_goods_cnt bigint          COMMENT '退款子订单量',
  refund_rate            decimal(14,4)   COMMENT '退款率',
  comment_cnt            bigint          COMMENT '评价数',
  good_comment_cnt       bigint          COMMENT '好评量',
  bad_comment_cnt        bigint          COMMENT '差评量',
  rank_index             bigint          COMMENT 'GCR倒排索引(根据gcr，gmv，销量降序排列)',
  region_id              string          COMMENT '国家/地区ID',  -- 新增
  collect_cnt            bigint          COMMENT '收藏数量',
  add_cat_cnt            bigint          COMMENT '加购次数'
) COMMENT '用户冷启动聚合数据' PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION "s3://vova-mlb/REC/data/match/new_user/pull_analysis_data"
STORED AS textfile;



@@@@@@
结果表:
--不区分brand产品召回结果
DROP TABLE mlb.mlb_vova_rec_m_nurecall_d;
create external TABLE mlb.mlb_vova_rec_m_nurecall_d
(
    cluster_key  string         COMMENT '分组标识',
    goods_id     bigint         COMMENT '商品id',
    rank_num     bigint         COMMENT '综合排序topN的rank索引'
) COMMENT '用户冷启动聚合数据(不区分brand)' PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION "s3://vova-mlb/REC/data/match/new_user/recall_result_data/rec_m_nurecall_d"
STORED AS textfile;

--非brand产品产召回结果
DROP TABLE mlb.mlb_vova_rec_m_nurecall_nb_d;
create external TABLE mlb.mlb_vova_rec_m_nurecall_nb_d
(
    cluster_key  string         COMMENT '分组标识',
    goods_id     bigint         COMMENT '商品id',
    rank_num     bigint         COMMENT '综合排序topN的rank索引'
) COMMENT '用户冷启动聚合数据(非brand)' PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION "s3://vova-mlb/REC/data/match/new_user/recall_result_data/rec_m_nurecall_nb_d"
STORED AS textfile;


create table rec_recall.ads_rec_m_nurecall_d (
    `id`           int(11) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
    `cluster_key`  varchar(100)  NOT NULL COMMENT '分组标识',
    `goods_id`     int(11)    NOT NULL COMMENT '商品id',
    `rank_num`     int(11)    NOT NULL COMMENT '综合排序topN的rank索引',
    PRIMARY KEY (`id`) USING BTREE,
    KEY cluster_key (cluster_key) USING BTREE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC COMMENT='用户冷启动聚合数据(不区分brand)';


create table rec_recall.ads_rec_m_nurecall_nb_d (
    `id`           int(11) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
    `cluster_key`  varchar(100)  NOT NULL COMMENT '分组标识',
    `goods_id`     int(11)    NOT NULL COMMENT '商品id',
    `rank_num`     int(11)    NOT NULL COMMENT '综合排序topN的rank索引',
    PRIMARY KEY (`id`) USING BTREE,
    KEY cluster_key (cluster_key) USING BTREE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC COMMENT='用户冷启动聚合数据(非brand)';

