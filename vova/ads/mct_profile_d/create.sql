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

