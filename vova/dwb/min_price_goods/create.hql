DROP TABLE dwb.dwb_vova_min_price_goods_summary;
CREATE EXTERNAL TABLE IF NOT EXISTS dwb.dwb_vova_min_price_goods_summary
(
    event_date             string COMMENT 'd_日期',
    tot_exposure           bigint COMMENT 'i_总曝光',
    tot_min_goods_exposure bigint COMMENT 'i_低价商品曝光',
    min_exopsure_rate      DECIMAL(15, 4) COMMENT 'i_低价商品曝光率'
) COMMENT '低价商品报表'
    PARTITIONED BY ( pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

DROP TABLE tmp.tmp_dwb_vova_min_goods_group;
CREATE EXTERNAL TABLE IF NOT EXISTS tmp.tmp_dwb_vova_min_goods_group
(
    goods_id             bigint COMMENT 'goods_id',
    min_price_goods_id   bigint COMMENT 'min_price_goods_id'
) COMMENT 'tmp_dwb_vova_min_goods_group'
  STORED AS PARQUETFILE;

DROP TABLE dwb.dwb_vova_min_price_goods_detail;
CREATE EXTERNAL TABLE IF NOT EXISTS dwb.dwb_vova_min_price_goods_detail
(
    event_date            string COMMENT 'd_日期',
    min_price_goods_id    bigint COMMENT 'i_最低价goodsid',
    first_cat_name        string COMMENT 'i_一级类目',
    shop_price_amount     DECIMAL(14, 2) COMMENT 'i_商品价格',
    mct_rank              string COMMENT 'i_最低价店铺等级',
    min_impression        bigint COMMENT 'i_曝光量',
    min_exopsure_rate     DECIMAL(15, 4) COMMENT 'i_最低价曝光比例',
    impression_rate_range string COMMENT 'i_最低价曝光比例区间',
    min_gmv_rate          DECIMAL(15, 4) COMMENT 'i_最低价gmv比例'
) COMMENT '最低价商品曝光量不足30%商品明细'
    PARTITIONED BY ( pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

DROP TABLE tmp.tmp_dwb_vova_min_price_goods_detail;
CREATE EXTERNAL TABLE IF NOT EXISTS tmp.tmp_dwb_vova_min_price_goods_detail
(
    event_date            string COMMENT 'd_日期',
    min_price_goods_id    bigint COMMENT 'min_price_goods_id',
    tot_impression        bigint COMMENT 'tot_impression',
    min_impression        bigint COMMENT 'min_impression',
    min_exopsure_rate     DECIMAL(15, 4) COMMENT 'min_exopsure_rate',
    impression_rate_range string COMMENT 'impression_rate_range',
    shop_price_amount     DECIMAL(14, 2) COMMENT 'shop_price_amount',
    tot_gmv               DECIMAL(14, 2) COMMENT 'tot_gmv',
    min_gmv               DECIMAL(14, 2) COMMENT 'min_gmv',
    min_gmv_rate          DECIMAL(15, 4) COMMENT 'min_gmv_rate',
    first_cat_name        string COMMENT 'first_cat_name',
    rank              string COMMENT 'mct_rank'
) COMMENT '最低价商品曝光量不足30%商品明细'
    STORED AS PARQUETFILE;




#history
hadoop distcp -Dmapreduce.map.memory.mb=8096 -m 40 -overwrite  hdfs://ha-nn-uri/user/hive/warehouse/rpt.db/rpt_min_price_goods_summary  s3://bigdata-offline/warehouse/dwb/dwb_vova_min_price_goods_summary_test

DROP TABLE dwb.dwb_vova_min_price_goods_summary_test;
CREATE EXTERNAL TABLE IF NOT EXISTS dwb.dwb_vova_min_price_goods_summary_test
(
    event_date             string COMMENT 'd_日期',
    tot_exposure           bigint COMMENT 'i_总曝光',
    tot_min_goods_exposure bigint COMMENT 'i_低价商品曝光',
    min_exopsure_rate      DECIMAL(15, 4) COMMENT 'i_低价商品曝光率'
) COMMENT '低价商品报表'
    PARTITIONED BY ( pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

MSCK REPAIR TABLE dwb.dwb_vova_min_price_goods_summary_test;

set hive.exec.dynamici.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT overwrite TABLE dwb.dwb_vova_min_price_goods_summary PARTITION (pt)
SELECT
/*+ REPARTITION(1) */
event_date,
tot_exposure,
tot_min_goods_exposure,
min_exopsure_rate,
pt
from dwb.dwb_vova_min_price_goods_summary_test
;
