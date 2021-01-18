vova集运项目监控报表

需求方及需求号：3689 罗宇清
创建时间及开发人员：2020-03-28 王琪
修改需求方及需求号：4544 罗宇清
修改人及修改时间：2020-06-11 韩俊涛

DROP TABLE rpt.rpt_collection_monitor;
CREATE TABLE IF NOT EXISTS rpt.rpt_collection_monitor
(
    action_date        date,
    goods_id           bigint,
    virtual_goods_id   bigint,
    goods_sn           string,
    shop_price         decimal(15, 2),
    shipping_fee       decimal(15, 2),
    shop_price_amount  decimal(15, 2),
    mct_name           string,
    is_on_sale         string,
    is_brand           string,
    special_attributes string,
    gmv                decimal(15, 2),
    sale_goods_cnt     bigint,
    exit_time          TIMESTAMP,
    is_flow            string
) COMMENT '热销产品退出集运监控报表' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;



DROP TABLE IF EXISTS rpt.rpt_monitor_cmb;
CREATE TABLE rpt.rpt_monitor_cmb
(
    region_code                         STRING COMMENT 'd_国家',
    event_date                          STRING COMMENT 'd_日期',
    tot_cnt                             bigint COMMENT 'i_已确认子订单数',
    my_cnt                              bigint COMMENT 'i_自营店铺订单数',
    oth_cnt                             bigint COMMENT 'i_商家订单数',
    my_no_brd_cnt                       bigint COMMENT 'i_自营非brand订单数',
    oth_no_brd_cnt                      bigint COMMENT 'i_商家非brand子订单数',
    sm_cnt                              bigint COMMENT 'i_集运子订单数',
    my_sm_cnt                           bigint COMMENT 'i_自营集运子订单数',
    oth_sm_cnt                          bigint COMMENT 'i_商家集运子订单数',
    tot_mor_pay_cnt                     bigint COMMENT 'i_额外付费子订单数',
    my_mor_pay_cnt                      bigint COMMENT 'i_自营额外付费子订单数',
    oth_mor_pay_cnt                     bigint COMMENT 'i_商家额外付费子订单数',
    tot_cmb_cnt                         bigint COMMENT 'i_合包数',
    sm_cnt_div_tot_cnt                  STRING COMMENT 'i_集运子订单占已确认订单比例',
    sm_cnt_div_no_brd_cnt               STRING COMMENT 'i_集运子订单占非brand订单比例',
    mor_pay_cnt_div_sm_cnt              STRING COMMENT 'i_额外付费子订单占集运子订单数比例',
    tot_mor_pay_amt_div_tot_mor_pay_cnt STRING COMMENT 'i_集运额外收费平均额',
    tot_mor_pay_amt                     bigint COMMENT 'i_集运额外收费总额'
) COMMENT '集运监控' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
2021-01-09

DROP TABLE IF EXISTS dwb.dwb_vova_monitor_cmb;
CREATE external TABLE dwb.dwb_vova_monitor_cmb
(
    region_code                         STRING COMMENT 'd_国家',
    event_date                          STRING COMMENT 'd_日期',
    tot_cnt                             bigint COMMENT 'i_已确认子订单数',
    my_cnt                              bigint COMMENT 'i_自营店铺订单数',
    oth_cnt                             bigint COMMENT 'i_商家订单数',
    my_no_brd_cnt                       bigint COMMENT 'i_自营非brand订单数',
    oth_no_brd_cnt                      bigint COMMENT 'i_商家非brand子订单数',
    sm_cnt                              bigint COMMENT 'i_集运子订单数',
    my_sm_cnt                           bigint COMMENT 'i_自营集运子订单数',
    oth_sm_cnt                          bigint COMMENT 'i_商家集运子订单数',
    tot_mor_pay_cnt                     bigint COMMENT 'i_额外付费子订单数',
    my_mor_pay_cnt                      bigint COMMENT 'i_自营额外付费子订单数',
    oth_mor_pay_cnt                     bigint COMMENT 'i_商家额外付费子订单数',
    tot_cmb_cnt                         bigint COMMENT 'i_合包数',
    sm_cnt_div_tot_cnt                  STRING COMMENT 'i_集运子订单占已确认订单比例',
    sm_cnt_div_no_brd_cnt               STRING COMMENT 'i_集运子订单占非brand订单比例',
    mor_pay_cnt_div_sm_cnt              STRING COMMENT 'i_额外付费子订单占集运子订单数比例',
    tot_mor_pay_amt_div_tot_mor_pay_cnt STRING COMMENT 'i_集运额外收费平均额',
    tot_mor_pay_amt                     bigint COMMENT 'i_集运额外收费总额',
    sm_gmv                              string COMMENT '集运子订单gmv',
    gmv_rate                            string COMMENT 'gmv占比',
    my_sku                              string COMMENT '集运子订单sku数量'
) COMMENT '集运监控' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE
LOCATION "s3://bigdata-offline/warehouse/dwb/dwb_vova_monitor_cmb/"
;