drop table dwb.dwb_vova_mct_rank;
CREATE external TABLE IF NOT EXISTS dwb.dwb_vova_mct_rank
(
    event_date                  date  COMMENT '日期',
    rank                        bigint COMMENT '商家等级',
    first_cat_id                bigint COMMENT '一级品类id',
    mct_cnt                     bigint COMMENT '该等级商家数量',
    dau                         bigint COMMENT 'dau',
    payed_order_num             bigint COMMENT '支付成功订单量',
    gmv                         double COMMENT 'gmv',
    payed_uv                    bigint COMMENT '支付人数',
    ct_dau                      bigint COMMENT 'ct_dau'
) COMMENT '商家等级报表'
PARTITIONED BY ( pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;


drop table dwb.dwb_vova_mct_rank_detail;
CREATE TABLE IF NOT EXISTS dwb.dwb_vova_mct_rank_detail
(
    event_date                  date  COMMENT '日期',
    rank                        bigint COMMENT '商家等级',
    first_cat_id                bigint COMMENT '一级品类id',
    mct_id                      bigint COMMENT '商家id',
    gmv_rank                    bigint COMMENT '商家gmv等级',
    bs_inter_rate_3_6w          double COMMENT '七天伤亡率',
    bs_lrf_rate_9_12w           double COMMENT '物流退款率',
    bs_nlrf_rate_5_8w           double COMMENT '非物流退款率',
    bs_rep_rate_1mth            double COMMENT '复购率',
    payed_order_num             bigint COMMENT '支付成功订单量',
    gmv                         double COMMENT 'gmv',
    dau                         bigint COMMENT 'dau',
    ct_dau                      bigint COMMENT 'ct_dau'
) COMMENT '商家等级报表'
PARTITIONED BY ( pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;



drop table tmp.rpt_mct_rank_detail_country_cr;
CREATE TABLE IF NOT EXISTS tmp.rpt_mct_rank_detail_country_cr
(
    country                     string COMMENT '国家',
    rate                        double COMMENT '转化率'
) COMMENT '国家转化率'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;