DROP TABLE dwb.dwb_vova_merchant_kpi;
CREATE EXTERNAL TABLE IF NOT EXISTS dwb.dwb_vova_merchant_kpi
(
    action_month   date,
    spsor          string,
    reg_cnt        bigint,
    activate_cnt   bigint,
    publish_cnt    bigint,
    paid_order_cnt bigint,
    paid_gmv       decimal(15, 4),
    paid_order_cnt_30 bigint,
    paid_order_cnt_1 bigint,
    paid_order_cnt_0 bigint
) COMMENT '招商kpi' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

DROP TABLE dwb.dwb_vova_merchant_gmv;
CREATE EXTERNAL TABLE IF NOT EXISTS dwb.dwb_vova_merchant_gmv
(
    spsor               string,
    first_publish_month date,
    pay_month           date,
    paid_gmv            decimal(15, 4)
) COMMENT '招商kpi' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

DROP TABLE dwb.dwb_vova_new_merchant_sponsor_gmv_m;
CREATE EXTERNAL TABLE IF NOT EXISTS dwb.dwb_vova_new_merchant_sponsor_gmv_m
(
    event_date                  date,
    spsor_name                  string,
    reg_cnt                     bigint,
    activate_cnt                bigint,
    gmv                         decimal(15, 2),
    not_brand_gmv               decimal(15, 2),
    gmv_m                       decimal(20, 2),
    not_brand_gmv_m             decimal(20, 2)
) COMMENT 'dwb_vova_new_merchant_sponsor_gmv_m' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

DROP TABLE dwb.dwb_vova_new_merchant_gmv_d;
CREATE EXTERNAL TABLE IF NOT EXISTS dwb.dwb_vova_new_merchant_gmv_d
(
    event_date                  date,
    spsor_name                  string,
    mct_name                    string,
    activate_time               TIMESTAMP,
    on_sale_cnt                 bigint,
    goods_gsn_cnt               bigint,
    goods_cnt                   bigint,
    gmv                         decimal(15, 2),
    not_brand_gmv               decimal(15, 2)
) COMMENT 'dwb_vova_new_merchant_gmv_d' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

