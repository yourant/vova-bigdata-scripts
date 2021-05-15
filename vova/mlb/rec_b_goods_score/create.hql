[8229]构建商品评分并应用于搜索及mostpopular
https://zt.gitvv.com/index.php?m=task&f=view&taskID=30625
任务描述
数据需求连接：https://docs.google.com/spreadsheets/d/1wNKP3vDer9Qop0Y0JvhzoCOl9m8DL4_HHWgRIiq-dvw/edit#gid=0

需求背景：
构建多维度的商品评分，用于推荐及搜索，以提升转化

需求内容：
1.通过商品数据构建以下维度的商品评分
    商品热度分：参考商品销量、gmv、曝光量、点击量、加车量、评价数
    商品转化分：参考ctr、gr、gcr
    商品销售表现分：综合以上两个评分
    商品履约分：参考七天上网率、物流退款率、非物流退款率、评价星级
    商品属性分：参考新品评分、商品等级、价格、库存充足度
    以上评分需为商品在全站/一级类目下的评分
2.基于以上评分，加权计算商品的全站/一级类目综合评分
3.将以上商品评分写入es，用于搜索和mostpopular的es权重计算，具体公式在评分完成后给到。功能需进行ab，新老版本各50%

数据需求见：https://docs.google.com/spreadsheets/d/1wNKP3vDer9Qop0Y0JvhzoCOl9m8DL4_HHWgRIiq-dvw/edit?ts=6010ff1d#gid=0


# 2021-02-01
# 综合评分表
ads.ads_rec_b_goods_score_d
  字段               类型              说明
  goods_id          bigint            商品id
  base_score        decimal(10,4)     基础评分
  hot_score         decimal(10,4)     热度评分
  conversion_score  decimal(10,4)     转化评分
  honor_score       decimal(10,4)     履约评分
  overall_score     decimal(10,4)     综合评分
  pt                string            分区
位置：s3://vova-mlb/REC/data/goods_score_data/goods_score/

drop table ads.ads_rec_b_goods_score_d;
CREATE external TABLE ads.ads_rec_b_goods_score_d (
  goods_id          bigint        COMMENT '商品id',
  base_score        DOUBLE        COMMENT '基础评分',
  hot_score         DOUBLE        COMMENT '热度评分',
  conversion_score  DOUBLE        COMMENT '转化评分',
  honor_score       DOUBLE        COMMENT '履约评分',
  overall_score     DOUBLE        COMMENT '综合评分'
) COMMENT '商品综合评分表(搜索及mostpopular)' PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION "s3://vova-mlb/REC/data/goods_score_data/goods_score/"
STORED AS textfile;

drop table ads_rec_b_goods_score_d;
CREATE TABLE `ads_rec_b_goods_score_d` (
  `id`                bigint(20)  NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `goods_id`          int(11)     NOT NULL COMMENT '商品id',
  `base_score`        DOUBLE      NOT NULL COMMENT '基础评分',
  `hot_score`         DOUBLE      NOT NULL COMMENT '热度评分',
  `conversion_score`  DOUBLE      NOT NULL COMMENT '转化评分',
  `honor_score`       DOUBLE      NOT NULL COMMENT '履约评分',
  `overall_score`     DOUBLE      NOT NULL COMMENT '综合评分',
  `update_time`       datetime    DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE KEY goods_id (goods_id) USING BTREE
  ) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC COMMENT='商品综合评分表(搜索及mostpopular)'
;



# 一级品类评分表
ads.ads_rec_b_catgoods_score_d
  字段                     类型            说明
  goods_id                bigint          商品id
  base_cat_score          decimal(10,4)   一级品类基础评分
  hot_cat_score           decimal(10,4)   一级品类热度评分
  conversion_cat_score    decimal(10,4)   一级品类转化评分
  honor_cat_score         decimal(10,4)   一级品类履约评分
  overall_cat_score       decimal(10,4)   一级品类综合评分
  pt                      string          分区
位置：s3://vova-mlb/REC/data/goods_score_data/goods_cat_score/

drop table ads.ads_rec_b_catgoods_score_d;
CREATE external TABLE ads.ads_rec_b_catgoods_score_d
(
    goods_id             bigint        COMMENT '商品id',
    base_cat_score       DOUBLE        COMMENT '一级品类基础评分',
    hot_cat_score        DOUBLE        COMMENT '一级品类热度评分',
    conversion_cat_score DOUBLE        COMMENT '一级品类转化评分',
    honor_cat_score      DOUBLE        COMMENT '一级品类履约评分',
    overall_cat_score    DOUBLE        COMMENT '一级品类综合评分'
) COMMENT '商品一级品类综合评分表(搜索及mostpopular)' PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION "s3://vova-mlb/REC/data/goods_score_data/goods_cat_score/"
STORED AS textfile;

drop table ads_rec_b_catgoods_score_d;
CREATE TABLE `ads_rec_b_catgoods_score_d` (
    `id`                    bigint(20)  NOT NULL AUTO_INCREMENT COMMENT '自增主键',
    `goods_id`              int(11)     NOT NULL COMMENT '商品id',
    `base_cat_score`        DOUBLE      NOT NULL COMMENT '一级品类基础评分',
    `hot_cat_score`         DOUBLE      NOT NULL COMMENT '一级品类热度评分',
    `conversion_cat_score`  DOUBLE      NOT NULL COMMENT '一级品类转化评分',
    `honor_cat_score`       DOUBLE      NOT NULL COMMENT '一级品类履约评分',
    `overall_cat_score`     DOUBLE      NOT NULL COMMENT '一级品类综合评分',
    `update_time`           datetime    DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`id`) USING BTREE,
    UNIQUE KEY goods_id (goods_id) USING BTREE
    ) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC COMMENT='商品一级品类综合评分表(搜索及mostpopular)'
;


# 迁移
drop table mlb.mlb_vova_rec_goods_scorebase_data_d
CREATE external TABLE mlb.mlb_vova_rec_goods_scorebase_data_d (
  goods_id            bigint        COMMENT '商品id',
  first_cat_id        bigint        COMMENT '一级品类id',
  second_cat_id       bigint        COMMENT '二级品类id',
  total_price         double        COMMENT '商品总价',
  mct_id              bigint        COMMENT '商家id',
  comment_cnt_6m      bigint        COMMENT '总评论数',
  comment_good_cnt_6m bigint        COMMENT '好评数',
  gmv_15d             double        COMMENT 'gmv',
  sales_vol_15d       bigint        COMMENT '销量',
  expre_cnt_15d       bigint        COMMENT '曝光量',
  clk_cnt_15d         bigint        COMMENT '点击量',
  collect_cnt_15d     bigint        COMMENT '收藏量',
  add_cat_cnt_15d     bigint        COMMENT '加购量',
  inter_rate_3_6w     double        COMMENT '七天上网率',
  lrf_rate_9_12w      double        COMMENT '物流退款率',
  nlrf_rate_5_8w      double        COMMENT '非物流退款率',
  bs_inter_rate_3_6w  double        COMMENT '平滑七天上网率',
  bs_lrf_rate_9_12w   double        COMMENT '平滑物流退款率',
  bs_nlrf_rate_5_8w   double        COMMENT '平滑非物流退款率',
  clk_uv_15d          bigint        COMMENT '点击UV',
  mct_score           bigint        COMMENT '商家一级评分'
) COMMENT '商品评分所用数据' PARTITIONED BY (pt STRING)
STORED AS PARQUET
LOCATION "s3://vova-mlb/REC/data/goods_score_data/goods_score_base_data/"
TBLPROPERTIES ('parquet.compress'='SNAPPY');

sh /mnt/vova-bigdata-scripts/common/job_message_put.sh --jname=ads_rec_goods_scorebase_data_d --from=data --to=java_server --jtype=1D --retry=0

drop table mlb.mlb_vova_rec_b_goods_score_d;
CREATE external TABLE mlb.mlb_vova_rec_b_goods_score_d (
  goods_id          bigint        COMMENT '商品id',
  base_score        DOUBLE        COMMENT '基础评分',
  hot_score         DOUBLE        COMMENT '热度评分',
  conversion_score  DOUBLE        COMMENT '转化评分',
  honor_score       DOUBLE        COMMENT '履约评分',
  overall_score     DOUBLE        COMMENT '综合评分'
) COMMENT '商品综合评分表(搜索及mostpopular)' PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION "s3://vova-mlb/REC/data/goods_score_data/goods_score/"
STORED AS textfile;

drop table mlb.mlb_vova_rec_b_catgoods_score_d;
CREATE external TABLE mlb.mlb_vova_rec_b_catgoods_score_d
(
    goods_id             bigint        COMMENT '商品id',
    base_cat_score       DOUBLE        COMMENT '一级品类基础评分',
    hot_cat_score        DOUBLE        COMMENT '一级品类热度评分',
    conversion_cat_score DOUBLE        COMMENT '一级品类转化评分',
    honor_cat_score      DOUBLE        COMMENT '一级品类履约评分',
    overall_cat_score    DOUBLE        COMMENT '一级品类综合评分'
) COMMENT '商品一级品类综合评分表(搜索及mostpopular)' PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION "s3://vova-mlb/REC/data/goods_score_data/goods_cat_score/"
STORED AS textfile;



########################################################
[9217]推荐管理平台新增--商品评分查询模块

https://confluence.gitvv.com/pages/viewpage.action?pageId=21268801

desc mlb.mlb_vova_rec_goods_scorebase_data_d;
desc mlb.mlb_vova_b_goods_score_details_d;
desc mlb.mlb_vova_b_goods_cat_score_details_d;
desc mlb.mlb_vova_rec_b_goods_score_d;
desc mlb.mlb_vova_rec_b_catgoods_score_d;
合并五张表, 结果输出 ES

CREATE TABLE IF NOT EXISTS mlb.mlb_vova_rec_b_goods_score_all_d(
goods_id            bigint COMMENT '商品id',

first_cat_id        bigint COMMENT '一级品类id',
second_cat_id       bigint COMMENT '二级品类id',
total_price         double COMMENT '商品总价',
mct_id              bigint COMMENT '商家id',
comment_cnt_6m      bigint COMMENT '总评论数',
comment_good_cnt_6m bigint COMMENT '好评数',
gmv_15d             double COMMENT 'gmv',
sales_vol_15d       bigint COMMENT '销量',
expre_cnt_15d       bigint COMMENT '曝光量',
clk_cnt_15d         bigint COMMENT '点击量',
collect_cnt_15d     bigint COMMENT '收藏量',
add_cat_cnt_15d     bigint COMMENT '加购量',
inter_rate_3_6w     double COMMENT '七天上网率',
lrf_rate_9_12w      double COMMENT '物流退款率',
nlrf_rate_5_8w      double COMMENT '非物流退款率',
bs_inter_rate_3_6w  double COMMENT '平滑七天上网率',
bs_lrf_rate_9_12w   double COMMENT '平滑物流退款率',
bs_nlrf_rate_5_8w   double COMMENT '平滑非物流退款率',
clk_uv_15d          bigint COMMENT '点击UV',
mct_score           bigint COMMENT '商家一级评分',

price_score             decimal(10,4)   COMMENT '价格评分',
-- mct_score               decimal(10,4)   COMMENT '商家一级评分',
good_cm_rate_score      decimal(10,4)   COMMENT '商品好评率评分',
good_cm_cnt_score       decimal(10,4)   COMMENT '评论量评分',
good_expre_cnt_score    decimal(10,4)   COMMENT '曝光量评分',
good_clk_cnt_score      decimal(10,4)   COMMENT '点击量评分',
good_cart_cnt_score     decimal(10,4)   COMMENT '加购量评分',
good_collect_cnt_score  decimal(10,4)   COMMENT '收藏量评分',
good_sale_vol_score     decimal(10,4)   COMMENT '销量评分',
inter_rate_score        decimal(10,4)   COMMENT '商品七天上网率评分',
nlrf_rate_score         decimal(10,4)   COMMENT '商品非物流退款率评分',
lrf_rate_score          decimal(10,4)   COMMENT '商品物流退款率评分',
gcr_score               decimal(10,4)   COMMENT '商品gcr评分',
gr_score                decimal(10,4)   COMMENT '商品gr评分',
ctr_score               decimal(10,4)   COMMENT '商品ctr评分',

-- price_score                 decimal(10,4)   COMMENT '价格评分',
-- mct_score                   decimal(10,4)   COMMENT '商家一级评分',
good_cm_rate_cat_score      decimal(10,4)   COMMENT '商品一级品类下好评率评分',
good_cm_cnt_cat_score       decimal(10,4)   COMMENT '商品一级品类下评论量评分',
good_expre_cnt_cat_score    decimal(10,4)   COMMENT '商品一级品类下曝光量评分',
good_clk_cnt_cat_score      decimal(10,4)   COMMENT '商品一级品类下点击量评分',
good_cart_cnt_cat_score     decimal(10,4)   COMMENT '商品一级品类下加购量评分',
good_collect_cnt_cat_score  decimal(10,4)   COMMENT '商品一级品类下收藏量评分',
good_sale_vol_cat_score     decimal(10,4)   COMMENT '商品一级品类下销量评分',
-- inter_rate_score            decimal(10,4)   COMMENT '商品七天上网率评分',
-- nlrf_rate_score             decimal(10,4)   COMMENT '商品非物流退款率评分',
-- lrf_rate_score              decimal(10,4)   COMMENT '商品物流退款率评分',
gcr_cat_score               decimal(10,4)   COMMENT '商品一级品类下gcr评分',
gr_cat_score                decimal(10,4)   COMMENT '商品一级品类下gr评分',
ctr_cat_score               decimal(10,4)   COMMENT '商品一级品类下ctr评分',

base_score            double  COMMENT '基础评分',
hot_score             double  COMMENT '热度评分',
conversion_score      double  COMMENT '转化评分',
honor_score           double  COMMENT '履约评分',
overall_score         double  COMMENT '综合评分',

base_cat_score        double  COMMENT '一级品类基础评分',
hot_cat_score         double  COMMENT '一级品类热度评分',
conversion_cat_score  double  COMMENT '一级品类转化评分',
honor_cat_score       double  COMMENT '一级品类履约评分',
overall_cat_score     double  COMMENT '一级品类综合评分',
inter_days_score      double  COMMENT '平滑上网天数评分'

) COMMENT '商品评分汇总' PARTITIONED BY (pt STRING)
    STORED AS PARQUETFILE;

###########################################
[9380] 推荐管理平台--商品评分查询模块新增逻辑-添加指标
https://zt.gitvv.com/index.php?m=task&f=view&taskID=34369
1. 商品评分查询增加评分标准字段：平滑上网天数评分 = 商品7天上网率评分+平均上网天数加分值
上网天数权重值计算逻辑：有7天上网率的商品计算平均上网天数，平均上网天数 = 7天再往前一个月的确认订单的平均上网天数
按照平均上网天数进行加分
若平均上网天数为1时，+4分
若平均上网天数为2时，+3分
若平均上网天数为3时，+2分
若平均上网天数为4时，+1分
若平均上网天数为5时，+0.75分
若平均上网天数为6时，+0.5分
若平均上网天数>7时，+0分

分数加满到100为止
alter table mlb.mlb_vova_rec_goods_scorebase_data_d add columns(`avg_inter_days_3_6w` int comment '商品平均上网天数') cascade;

mlb.mlb_vova_rec_b_goods_score_all_d
alter table mlb.mlb_vova_rec_b_goods_score_all_d add columns(`inter_days_score` double comment '平滑上网天数评分') cascade;

alter table mlb.mlb_vova_rec_b_goods_score_all_d add columns(`first_cat_name` string comment '一级品类名称') cascade;
alter table mlb.mlb_vova_rec_b_goods_score_all_d add columns(`second_cat_name` string comment '二级品类名称') cascade;

alter table mlb.mlb_vova_rec_b_goods_score_all_d add columns(`avg_inter_days_3_6w` int comment '商品平均上网天数') cascade;






