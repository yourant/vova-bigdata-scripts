DROP TABLE ads.ads_zq_fn_picture_puzzle_w;
CREATE TABLE IF NOT EXISTS ads.ads_zq_fn_picture_puzzle_w
(
    img_id                   bigint,
    goods_id                 bigint,
    virtual_goods_id         bigint,
    first_cat_id             bigint,
    first_cat_name           string,
    second_cat_id            bigint,
    second_cat_name          string,
    img_color                string,
    img_url                  string,
    img_original             string,
    is_default               bigint
) COMMENT 'fn_ads_picture_puzzle_w' PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE location 's3://vomkt-emr-rec/data/fn_picture_puzzle';


DROP TABLE ads.ads_zq_fn_picture_puzzle_v2_w;
CREATE TABLE IF NOT EXISTS ads.ads_zq_fn_picture_puzzle_v2_w
(
    img_id                   bigint,
    goods_id                 bigint,
    virtual_goods_id         bigint,
    first_cat_id             bigint,
    first_cat_name           string,
    second_cat_id            bigint,
    second_cat_name          string,
    img_color                string,
    img_url                  string,
    img_original             string,
    is_default               bigint,
    location_id              bigint,
    datasource               string
) COMMENT 'fn_ads_picture_puzzle_v2_w' PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE location 's3://vomkt-emr-rec/data/fn_picture_puzzle_v2';

DROP TABLE ads.ads_zq_fn_picture_puzzle_v2;
CREATE TABLE IF NOT EXISTS ads.ads_zq_fn_picture_puzzle_v2
(
    img_id                   bigint,
    goods_id                 bigint,
    virtual_goods_id         bigint,
    first_cat_id             bigint,
    first_cat_name           string,
    second_cat_id            bigint,
    second_cat_name          string,
    img_color                string,
    img_url                  string,
    img_original             string,
    is_default               bigint,
    location_id              bigint,
    datasource               string
) COMMENT 'fn_ads_picture_puzzle_w'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE location 's3://vomkt-emr-rec/data/fn_picture_puzzle_no_pt';

DROP TABLE tmp.tmp_zq_0113_1;
CREATE TABLE IF NOT EXISTS tmp.tmp_zq_0113_1 as
select
img_id,
goods_id,
virtual_goods_id,
first_cat_id,
first_cat_name,
second_cat_id,
second_cat_name,
img_color,
img_url,
img_original,
is_default,
location_id,
pt
from ads.ads_zq_fn_picture_puzzle_v2_w where pt< '2021-01-13';



set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE ads.ads_zq_fn_picture_puzzle_v2_w PARTITION (pt)
select
/*+ REPARTITION(1) */
img_id,
goods_id,
virtual_goods_id,
first_cat_id,
first_cat_name,
second_cat_id,
second_cat_name,
img_color,
img_url,
img_original,
is_default,
location_id,
'florynight' as datasource,
pt
from tmp.tmp_zq_0113_1;



