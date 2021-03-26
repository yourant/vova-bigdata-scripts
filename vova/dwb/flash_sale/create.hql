DROP TABLE dwb.dwb_vova_market_daily_flash_sale;
CREATE EXTERNAL TABLE dwb.dwb_vova_market_daily_flash_sale
(
    event_date                   date,
    region_code                  string,
    platform                     string,
    market_paid_order_num        bigint,
    market_paid_buyer_num        bigint,
    market_paid_goods_num        bigint,
    market_order_order_num       bigint,
    market_order_user_num        bigint,
    flash_sale_goods_gmv         decimal(15, 2),
    flash_sale_order_info_gmv    decimal(15, 2),
    market_order_again_order_num bigint,
    market_paid_again_order_num  bigint,
    market_gmv                   decimal(15, 2),
    datasource                   string
) COMMENT 'flash-market' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

DROP TABLE dwb.dwb_vova_daily_flash_sale;
CREATE EXTERNAL TABLE dwb.dwb_vova_daily_flash_sale
(
    event_date                     date,
    region_code                    string,
    platform                       string,
    market_paid_order_num          bigint,
    market_paid_buyer_num          bigint,
    market_paid_goods_num          bigint,
    market_order_order_num         bigint,
    market_order_user_num          bigint,
    flash_sale_goods_gmv           decimal(15, 2),
    flash_sale_order_info_gmv      decimal(15, 2),
    market_order_again_order_num   bigint,
    market_paid_again_order_num    bigint,
    market_gmv                     decimal(15, 2),
    on_sale_uv                     bigint,
    on_sale_pv                     bigint,
    upcoming_uv                    bigint,
    upcoming_pv                    bigint,
    homepage_uv                    bigint,
    onsale_produce_detail_cnt_uv   bigint,
    onsale_produce_detail_cnt_pv   bigint,
    upcoming_produce_detail_cnt_uv bigint,
    upcoming_produce_detail_cnt_pv bigint,
    list_add_bag_cnt_uv            bigint,
    list_add_bag_cnt_pv            bigint,
    product_detail_cnt_uv          bigint,
    product_detail_cnt_pv          bigint,
    click_uv                       bigint,
    impression_uv                  bigint,
    cohort_1                       bigint,
    cohort_7                       bigint,
    merchant_total_goods_cnt       bigint,
    cur_sale_flashsale_goods_cnt   bigint,
    market_dau                     bigint,
    datasource                     string
) COMMENT 'flash_market' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

DROP TABLE dwb.dwb_vova_flash_sale_per_hour;
CREATE EXTERNAL TABLE dwb.dwb_vova_flash_sale_per_hour
(
    event_date    date,
    region_code   STRING,
    platform      STRING,
    activity_name STRING,
    field_name    STRING,
    hour_0        decimal(15, 2),
    hour_1        decimal(15, 2),
    hour_2        decimal(15, 2),
    hour_3        decimal(15, 2),
    hour_4        decimal(15, 2),
    hour_5        decimal(15, 2),
    hour_6        decimal(15, 2),
    hour_7        decimal(15, 2),
    hour_8        decimal(15, 2),
    hour_9        decimal(15, 2),
    hour_10       decimal(15, 2),
    hour_11       decimal(15, 2),
    hour_12       decimal(15, 2),
    hour_13       decimal(15, 2),
    hour_14       decimal(15, 2),
    hour_15       decimal(15, 2),
    hour_16       decimal(15, 2),
    hour_17       decimal(15, 2),
    hour_18       decimal(15, 2),
    hour_19       decimal(15, 2),
    hour_20       decimal(15, 2),
    hour_21       decimal(15, 2),
    hour_22       decimal(15, 2),
    hour_23       decimal(15, 2)
) COMMENT 'rpt_flash_sale_per_hour' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

select pt,count(*),count(distinct event_date,region_code,platform) from dwb.dwb_vova_daily_flash_sale_new group by pt;
DROP TABLE dwb.dwb_vova_daily_flash_sale_new;
CREATE EXTERNAL TABLE dwb.dwb_vova_daily_flash_sale_new
(
    event_date                     date,
    region_code                    string,
    platform                       string,
    market_paid_order_num          bigint,
    market_paid_buyer_num          bigint,
    market_order_order_num         bigint,
    market_order_user_num          bigint,
    flash_sale_goods_gmv           decimal(15, 2),
    flash_sale_order_info_gmv      decimal(15, 2),
    market_order_again_order_num   bigint,
    market_paid_again_order_num    bigint,
    market_gmv                     decimal(15, 2),
    on_sale_uv                     bigint,
    upcoming_uv                    bigint,
    cohort_1                       bigint,
    market_dau                     bigint,
    datasource                     string
) COMMENT 'dwb_vova_daily_flash_sale_new' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

DROP TABLE dwb.dwb_vova_flash_sale_data;
CREATE EXTERNAL TABLE dwb.dwb_vova_flash_sale_data
(
    datasource        string COMMENT 'datasource',
    goods_id          bigint COMMENT '',
    sale_date         date COMMENT '',
    virtual_goods_id  bigint COMMENT '',
    first_cat_name    string COMMENT '',
    second_cat_name   string COMMENT '',
    shop_price        decimal(15, 2) COMMENT '',
    shipping_fee      decimal(15, 2) COMMENT '',
    order_user        bigint COMMENT '',
    order_num         bigint COMMENT '',
    pay_user          bigint COMMENT '',
    pay_num           bigint COMMENT '',
    gmv               decimal(15, 2) COMMENT '',
    goods_name        string COMMENT '',
    shop_price_amount decimal(15, 2) COMMENT '',
    gmv_div_pay_user  decimal(15, 4) COMMENT ''
) COMMENT 'flash_sale_data'
    PARTITIONED BY ( pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;









































#history
hadoop distcp -Dmapreduce.map.memory.mb=8096 -m 40 -overwrite  hdfs://ha-nn-uri/user/hive/warehouse/rpt.db/rpt_market_daily_flash_sale  s3://bigdata-offline/warehouse/tmp/rpt_market_daily_flash_sale
hadoop distcp -Dmapreduce.map.memory.mb=8096 -m 40 -overwrite  hdfs://ha-nn-uri/user/hive/warehouse/rpt.db/rpt_daily_flash_sale  s3://bigdata-offline/warehouse/tmp/rpt_daily_flash_sale
hadoop distcp -Dmapreduce.map.memory.mb=8096 -m 40 -overwrite  hdfs://ha-nn-uri/user/hive/warehouse/rpt.db/rpt_flash_sale_per_hour  s3://bigdata-offline/warehouse/tmp/rpt_flash_sale_per_hour
hadoop distcp -Dmapreduce.map.memory.mb=8096 -m 40 -overwrite  hdfs://ha-nn-uri/user/hive/warehouse/rpt.db/rpt_flash_sale_data  s3://bigdata-offline/warehouse/tmp/rpt_flash_sale_data

DROP TABLE tmp.rpt_market_daily_flash_sale;
CREATE EXTERNAL TABLE tmp.rpt_market_daily_flash_sale
(
    event_date                   date,
    region_code                  string,
    platform                     string,
    market_paid_order_num        bigint,
    market_paid_buyer_num        bigint,
    market_paid_goods_num        bigint,
    market_order_order_num       bigint,
    market_order_user_num        bigint,
    flash_sale_goods_gmv         decimal(15, 2),
    flash_sale_order_info_gmv    decimal(15, 2),
    market_order_again_order_num bigint,
    market_paid_again_order_num  bigint,
    market_gmv                   decimal(15, 2),
    datasource                   string
) COMMENT 'flash-market' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

DROP TABLE tmp.rpt_daily_flash_sale;
CREATE EXTERNAL TABLE tmp.rpt_daily_flash_sale
(
    event_date                     date,
    region_code                    string,
    platform                       string,
    market_paid_order_num          bigint,
    market_paid_buyer_num          bigint,
    market_paid_goods_num          bigint,
    market_order_order_num         bigint,
    market_order_user_num          bigint,
    flash_sale_goods_gmv           decimal(15, 2),
    flash_sale_order_info_gmv      decimal(15, 2),
    market_order_again_order_num   bigint,
    market_paid_again_order_num    bigint,
    market_gmv                     decimal(15, 2),
    on_sale_uv                     bigint,
    on_sale_pv                     bigint,
    upcoming_uv                    bigint,
    upcoming_pv                    bigint,
    homepage_uv                    bigint,
    onsale_produce_detail_cnt_uv   bigint,
    onsale_produce_detail_cnt_pv   bigint,
    upcoming_produce_detail_cnt_uv bigint,
    upcoming_produce_detail_cnt_pv bigint,
    list_add_bag_cnt_uv            bigint,
    list_add_bag_cnt_pv            bigint,
    product_detail_cnt_uv          bigint,
    product_detail_cnt_pv          bigint,
    click_uv                       bigint,
    impression_uv                  bigint,
    cohort_1                       bigint,
    cohort_7                       bigint,
    merchant_total_goods_cnt       bigint,
    cur_sale_flashsale_goods_cnt   bigint,
    market_dau                     bigint,
    datasource                     string
) COMMENT 'flash_market' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

DROP TABLE tmp.rpt_flash_sale_per_hour;
CREATE EXTERNAL TABLE tmp.rpt_flash_sale_per_hour
(
    event_date    date,
    region_code   STRING,
    platform      STRING,
    activity_name STRING,
    field_name    STRING,
    hour_0        decimal(15, 2),
    hour_1        decimal(15, 2),
    hour_2        decimal(15, 2),
    hour_3        decimal(15, 2),
    hour_4        decimal(15, 2),
    hour_5        decimal(15, 2),
    hour_6        decimal(15, 2),
    hour_7        decimal(15, 2),
    hour_8        decimal(15, 2),
    hour_9        decimal(15, 2),
    hour_10       decimal(15, 2),
    hour_11       decimal(15, 2),
    hour_12       decimal(15, 2),
    hour_13       decimal(15, 2),
    hour_14       decimal(15, 2),
    hour_15       decimal(15, 2),
    hour_16       decimal(15, 2),
    hour_17       decimal(15, 2),
    hour_18       decimal(15, 2),
    hour_19       decimal(15, 2),
    hour_20       decimal(15, 2),
    hour_21       decimal(15, 2),
    hour_22       decimal(15, 2),
    hour_23       decimal(15, 2)
) COMMENT 'rpt_flash_sale_per_hour' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

DROP TABLE tmp.rpt_flash_sale_data;
CREATE EXTERNAL TABLE tmp.rpt_flash_sale_data
(
    datasource        string COMMENT 'datasource',
    goods_id          bigint COMMENT '',
    sale_date         date COMMENT '',
    virtual_goods_id  bigint COMMENT '',
    first_cat_name    string COMMENT '',
    second_cat_name   string COMMENT '',
    shop_price        decimal(15, 2) COMMENT '',
    shipping_fee      decimal(15, 2) COMMENT '',
    order_user        bigint COMMENT '',
    order_num         bigint COMMENT '',
    pay_user          bigint COMMENT '',
    pay_num           bigint COMMENT '',
    gmv               decimal(15, 2) COMMENT '',
    goods_name        string COMMENT '',
    shop_price_amount decimal(15, 2) COMMENT '',
    gmv_div_pay_user  decimal(15, 4) COMMENT ''
) COMMENT 'flash_sale_data'
    PARTITIONED BY ( pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

MSCK REPAIR TABLE tmp.rpt_market_daily_flash_sale;
MSCK REPAIR TABLE tmp.rpt_daily_flash_sale;
MSCK REPAIR TABLE tmp.rpt_flash_sale_per_hour;
MSCK REPAIR TABLE tmp.rpt_flash_sale_data;

set hive.exec.dynamici.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT overwrite TABLE dwb.dwb_vova_market_daily_flash_sale PARTITION (pt)
SELECT
/*+ REPARTITION(1) */
event_date,
region_code                  ,
platform                     ,
market_paid_order_num        ,
market_paid_buyer_num        ,
market_paid_goods_num        ,
market_order_order_num       ,
market_order_user_num        ,
flash_sale_goods_gmv         ,
flash_sale_order_info_gmv    ,
market_order_again_order_num ,
market_paid_again_order_num  ,
market_gmv,
nvl(datasource,'vova') as datasource,
pt
from
tmp.rpt_market_daily_flash_sale
;


INSERT overwrite TABLE dwb.dwb_vova_daily_flash_sale PARTITION (pt)
SELECT
/*+ REPARTITION(1) */
    event_date                     ,
    region_code                    ,
    platform                       ,
    market_paid_order_num          ,
    market_paid_buyer_num          ,
    market_paid_goods_num          ,
    market_order_order_num         ,
    market_order_user_num          ,
    flash_sale_goods_gmv           ,
    flash_sale_order_info_gmv      ,
    market_order_again_order_num   ,
    market_paid_again_order_num    ,
    market_gmv                     ,
    on_sale_uv                     ,
    on_sale_pv                     ,
    upcoming_uv                    ,
    upcoming_pv                    ,
    homepage_uv                    ,
    onsale_produce_detail_cnt_uv   ,
    onsale_produce_detail_cnt_pv   ,
    upcoming_produce_detail_cnt_uv ,
    upcoming_produce_detail_cnt_pv ,
    list_add_bag_cnt_uv            ,
    list_add_bag_cnt_pv            ,
    product_detail_cnt_uv          ,
    product_detail_cnt_pv          ,
    click_uv                       ,
    impression_uv                  ,
    cohort_1                       ,
    cohort_7                       ,
    merchant_total_goods_cnt       ,
    cur_sale_flashsale_goods_cnt   ,
    market_dau                     ,
nvl(datasource,'vova') as datasource,
pt
from
tmp.rpt_daily_flash_sale
;

INSERT overwrite TABLE dwb.dwb_vova_flash_sale_per_hour PARTITION (pt)
SELECT
/*+ REPARTITION(1) */
    event_date    ,
    region_code   ,
    platform      ,
    activity_name ,
    field_name    ,
    hour_0        ,
    hour_1        ,
    hour_2        ,
    hour_3        ,
    hour_4        ,
    hour_5        ,
    hour_6        ,
    hour_7        ,
    hour_8        ,
    hour_9        ,
    hour_10       ,
    hour_11       ,
    hour_12       ,
    hour_13       ,
    hour_14       ,
    hour_15       ,
    hour_16       ,
    hour_17       ,
    hour_18       ,
    hour_19       ,
    hour_20       ,
    hour_21       ,
    hour_22       ,
    hour_23,
pt
from
tmp.rpt_flash_sale_per_hour
;

INSERT overwrite TABLE dwb.dwb_vova_flash_sale_data PARTITION (pt)
SELECT
/*+ REPARTITION(1) */
nvl(datasource,'vova') as datasource,
    goods_id          ,
    sale_date         ,
    virtual_goods_id  ,
    first_cat_name    ,
    second_cat_name   ,
    shop_price        ,
    shipping_fee      ,
    order_user        ,
    order_num         ,
    pay_user          ,
    pay_num           ,
    gmv               ,
    goods_name        ,
    shop_price_amount ,
    gmv_div_pay_user  ,
pt
from
tmp.rpt_flash_sale_data
;

