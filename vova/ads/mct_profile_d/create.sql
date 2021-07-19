--商品维度汇总表
drop table ads.ads_vova_mct_profile;
CREATE external TABLE IF NOT EXISTS ads.ads_vova_mct_profile
(
    mct_id                    BIGINT COMMENT '商品所属商家ID',
    first_cat_id              BIGINT COMMENT '一级品类目ID',
    is_new                    BIGINT COMMENT '是否新商家（商家注册到当天的时间间隔小于或等于90天(3个月))',
    reg_to_now_days           BIGINT COMMENT '商家注册到当天的间隔天数',
    on_sale_flag              BIGINT COMMENT '在售标签',
    average_price             DOUBLE COMMENT '商品均价',
    cur_gmv                   DOUBLE COMMENT 'gmv',
    cur_uv                    BIGINT COMMENT '商品点击uv',
    cur_pv                    BIGINT COMMENT '点击pv',
    cur_payed_uv              DOUBLE COMMENT '当天支付uv',
    cur_ctr                   DOUBLE COMMENT '当天点击率',
    bs_cur_ctr                DOUBLE COMMENT '当天点击率',
    cur_cr                    DOUBLE COMMENT '当天转化率',
    bs_cur_cr                 DOUBLE COMMENT '当天转化率',
    gmv_1m                    DOUBLE COMMENT '最近30天的gmv',
    atv_2m                    DOUBLE COMMENT '最近60天的客单价',
    inter_rate_3_6w           DOUBLE COMMENT '前42天到前21天的订单7天上网率',
    bs_inter_rate_3_6w        DOUBLE COMMENT '前42天到前21天的订单7天上网率',
    lrf_rate_9_12w            DOUBLE COMMENT '前84天至前63天订单的物流退款率',
    bs_lrf_rate_9_12w         DOUBLE COMMENT '前84天至前63天订单的物流退款率',
    nlrf_rate_5_8w            DOUBLE COMMENT '前56天至前35天订单的非物流退款率',
    bs_nlrf_rate_5_8w         DOUBLE COMMENT '前56天至前35天订单的非物流退款率',
    rep_rate_1mth             DOUBLE COMMENT '月复购率',
    bs_rep_rate_1mth          DOUBLE COMMENT '月复购率',
    cohort_rate_1mth          DOUBLE COMMENT '月留存购率',
    bs_cohort_rate_1mth       DOUBLE COMMENT '月留存购率',
    rf_rate_1_3m              DOUBLE COMMENT '前90天至前30天订单的退款率',
    bs_rf_rate_1_3m           DOUBLE COMMENT '前90天至前30天订单的退款率',
    proper_rate_5_8w          DOUBLE COMMENT '前56天至前35天订单的30天妥投率',
    bs_proper_rate_5_8w       DOUBLE COMMENT '前56天至前35天订单的30天妥投率',
    uv_1m                     BIGINT COMMENT '最近三十天uv',
    payed_uv_1m               BIGINT COMMENT '最近三十天支付uv',
    bs_avg_cr_1m              DOUBLE COMMENT '近30天平均转化率',
    sell_goods_cnt_1m         BIGINT COMMENT '近30天有销量商品数量',
    on_sale_goods_cnt_1m      BIGINT COMMENT '近30天在售商品数量',
    turnover_rate_1m          DOUBLE COMMENT '近30天动销率',
    bs_turnover_rate_1m       DOUBLE COMMENT '近30天动销率'
) COMMENT '商家一级品类画像'
 PARTITIONED BY ( pt string)  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

 --商品维度汇总表
drop table ads.ads_vova_on_sale_goods_d;
CREATE external TABLE IF NOT EXISTS ads.ads_vova_on_sale_goods_d
(
    goods_id                  BIGINT COMMENT '商品所属商家ID',
    mct_id                    BIGINT COMMENT '商品所属商家ID',
    first_cat_id              BIGINT COMMENT '一级品类目ID'
) COMMENT '每日在售商品表'
 PARTITIONED BY ( pt string)  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;


drop table ads.ads_vova_mct_rank;
CREATE external TABLE IF NOT EXISTS ads.ads_vova_mct_rank(
  first_cat_id          bigint COMMENT '一级品类ID',
  mct_id                bigint COMMENT '商家ID',
  gmv_1m                double COMMENT '近30天gmv',
  bs_inter_rate_3_6w    double COMMENT '7天上网率(贝叶斯)',
  bs_lrf_rate_9_12w     double COMMENT '物流退款率(贝叶斯)',
  bs_nlrf_rate_5_8w     double COMMENT '非物流退款率(贝叶斯)',
  bs_rep_rate_1mth      double COMMENT '回购率(贝叶斯)',
  gmv_rank              bigint COMMENT 'gmv等级',
  rank                  bigint COMMENT '最终等级',
  score                 bigint COMMENT '得分',
  inter_rate_3_6w       double COMMENT '7天上网率',
  lrf_rate_9_12w        double COMMENT '物流退款率',
  nlrf_rate_5_8w        double COMMENT '非物流退款率',
  rep_rate_1mth         double COMMENT '回购率'
) COMMENT '商家等级表' PARTITIONED BY (pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;



drop table ads.ads_vova_mct_score_sum;
CREATE  TABLE IF NOT EXISTS ads.ads_vova_mct_score_sum(
mct_id bigint COMMENT '商家ID',
total_score bigint COMMENT '总分',
loc_ratio double
) COMMENT '商家总分表'
PARTITIONED BY (pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;


###################################################################################
# [10086] 商家分级增加商品、履约、售后等相关指标
任务描述
需求背景：
完善商家分级维度，增加商品、履约、售后等相关指标。

需求描述：
（1）在商家分级中增加商家取消率、5天上网率（取代原7天上网率）、7天入库率、DSR物流评分、DSR商品评分、商家驳回申诉率指标，完善商家分级体系。（详细描述参见附件。）
（2）先取数进行调研，取数维度为商家+品类，详细见：https://docs.google.com/spreadsheets/d/1oXwqE6se-zBq3dHRXUms8i-Nj503ZsuLPbmHYghjX1k/edit#gid=0
（3）等到取数分析完后，将这些特征加到当前商家分级算法里。
新增指标名称  口径  所在表及字段
商家取消率   分母中商家退款的子订单数/14天前再往前一个月的确认子订单数  "退款原因为商家退款
Merchant Auto Cancel Order
Merchant Cancel Order
Merchant Cancel Shipped Order
对应的ID是5、6、11、14"
5天上网率   分母中（上网时间-确认时间）在5天内的子订单数/5天前再往前一个月的确认子订单数
7天入库率（仅集运订单有）   分母中入库时间减订单确认的时间小于7天的子订单数/7天前再往前一个月的集运确认子订单数 ods_vova_vts.ods_vova_collection_order_goods 的 in_warehouse_time
DSR物流评分 分母中的物流评分总和/2021.3.15之后有物流评分订单数  goods_comment.logistics_transportation_rating
DSR商品评分 分母中的商品评分总和/2021.3.15之后有商品评分订单数  goods_comment.rating
商家驳回申诉率 分母中商家驳回申诉子订单数/58天前再往前一个月的确认子订单数
"SELECT
(SELECT 1 FROM refund_audit_txn rat WHERE rat.order_goods_id=rr.order_goods_id AND audit_status='mct_audit_rejected' LIMIT 1 ) as mct_audit_rejected
(SELECT 1 FROM refund_audit_txn rat WHERE rat.order_goods_id=rr.order_goods_id AND recheck_type=2 LIMIT 1 ) as recheck_type
FROM refund_reason rr
HAVING mct_audit_rejected=1 AND recheck_type=1"


# 商家取消率、5天上网率（取代原7天上网率）、7天入库率、DSR物流评分、DSR商品评分、商家驳回申诉率
alter table ads.ads_vova_mct_profile add columns(mct_cancel_rate          double comment '商家取消率') cascade;
alter table ads.ads_vova_mct_profile add columns(bs_mct_cancel_rate       double comment '商家取消率') cascade;

alter table ads.ads_vova_mct_profile add columns(inter_rate_5d_rate       double comment '5天上网率') cascade;
alter table ads.ads_vova_mct_profile add columns(bs_inter_rate_5d_rate    double comment '5天上网率') cascade;

alter table ads.ads_vova_mct_profile add columns(in_collection_7d_rate    double comment '7天入库率') cascade;
alter table ads.ads_vova_mct_profile add columns(dsr_logistics_rate       double comment 'DSR物流评分') cascade;
alter table ads.ads_vova_mct_profile add columns(dsr_goods_rate           double comment 'DSR商品评分') cascade;
alter table ads.ads_vova_mct_profile add columns(mct_audit_rejected_rate  double comment '商家驳回申诉率') cascade;
####### 分子 分母
alter table ads.ads_vova_mct_profile add columns(mct_cancel_order_cnt     bigint comment '商家退款的子订单数') cascade;
alter table ads.ads_vova_mct_profile add columns(confirm_order_cnt_14d    bigint comment '14天前再往前一个月的确认子订单数') cascade;
alter table ads.ads_vova_mct_profile add columns(inter_order_cnt_5d       bigint comment '5天内上网的子订单数') cascade;
alter table ads.ads_vova_mct_profile add columns(confirm_order_cnt_5d     bigint comment '5天前再往前一个月的确认子订单数') cascade;

alter table ads.ads_vova_mct_profile add columns(collection_order_goods_cnt        bigint comment '商品集运总订单数') cascade;
alter table ads.ads_vova_mct_profile add columns(in_collection_72hour_order_cnt    bigint comment '72小时入库订单数') cascade;
alter table ads.ads_vova_mct_profile add columns(logistics_comment_order_goods_cnt bigint comment '2021.3.15之后有物流评分订单数') cascade;
alter table ads.ads_vova_mct_profile add columns(comment_order_goods_cnt  bigint comment '2021.3.15之后有商品评分订单数') cascade;
alter table ads.ads_vova_mct_profile add columns(logistics_rating_sum     bigint comment '物流评分总和') cascade;
alter table ads.ads_vova_mct_profile add columns(rating_sum               bigint comment '商品评分总和') cascade;
alter table ads.ads_vova_mct_profile add columns(order_goods_cnt_58d      bigint comment '58天前再往前一个月的确认子订单数') cascade;
alter table ads.ads_vova_mct_profile add columns(mar_order_goods_cnt      bigint comment '商家驳回申诉子订单数') cascade;

##############################
alter table ads.ads_vova_mct_profile add columns(bs_in_collection_7d_rate    double comment '7天入库率(bayes)') cascade;
alter table ads.ads_vova_mct_profile add columns(bs_dsr_logistics_rate       double comment 'DSR物流评分(bayes)') cascade;
alter table ads.ads_vova_mct_profile add columns(bs_dsr_goods_rate           double comment 'DSR商品评分(bayes)') cascade;


