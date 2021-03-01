DROP TABLE ads.ads_fn_rec_b_session_data_d;
create external TABLE ads.ads_fn_rec_b_session_data_d
(
    domain_userid    string,
    goods_id         bigint,
    session_id       string,
    derived_tstamp   string
) COMMENT 'ads_fn_rec_b_session_data_d' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS PARQUET
    LOCATION "s3://vova-mlb/FN-REC/data/base/rec_b_session_data_d";

MSCK REPAIR TABLE ads.ads_fn_rec_b_session_data_d;


DROP TABLE ads.ads_fn_rec_b_goods_map;
create external TABLE ads.ads_fn_rec_b_goods_map
(
    datasource       string,
    goods_id         bigint,
    fn_goods_id      bigint
) COMMENT 'ads_fn_rec_b_goods_map'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS PARQUET
    LOCATION "s3://vova-mlb/FN-REC/data/base/rec_b_goods_map";


