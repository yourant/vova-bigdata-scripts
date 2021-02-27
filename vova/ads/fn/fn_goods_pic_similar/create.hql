DROP TABLE ads.fn_ads_min_price_goods;
CREATE external TABLE ads.fn_ads_min_price_goods
(
    goods_id           bigint,
    min_price_goods_id bigint,
    strategy           string,
    group_number       string,
    min_show_price     decimal(14, 4)
) COMMENT 'fn_ads_min_price_goods' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

DROP TABLE ads.fn_ads_img_snapshot;
CREATE external TABLE ads.fn_ads_img_snapshot
(
    goods_id           bigint,
    img_id             bigint,
    img_url            string,
    img_original      string,
    last_update_time  TIMESTAMP
) COMMENT 'fn_ads_img_snapshot' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

DROP TABLE ads.fn_ads_img_snapshot_arc;
CREATE external TABLE ads.fn_ads_img_snapshot_arc
(
    goods_id           bigint,
    img_id             bigint,
    img_original       string
) COMMENT 'fn_ads_img_snapshot_arc' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    LOCATION "s3://vomkt-emr-rec/data/fn_goods_img/tab/fn_ads_img_snapshot_arc"
    STORED AS textfile;


DROP TABLE ads.fn_ads_img_vector_arc;
CREATE external TABLE ads.fn_ads_img_vector_arc
(
    goods_id           bigint,
    img_id             bigint,
    img_original       string,
    img_vector         string
) COMMENT 'fn_ads_img_vector_arc' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    LOCATION "s3://vomkt-emr-rec/data/fn_goods_img/img_vector"
    STORED AS textfile;

DROP TABLE ads.fn_ads_img_vector;
CREATE external TABLE ads.fn_ads_img_vector
(
    event_date        string,
    goods_id          bigint,
    img_id            bigint,
    img_original      string,
    img_vector        string
) COMMENT 'fn_ads_img_vector' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

MSCK REPAIR TABLE ads.fn_ads_img_snapshot_arc;
MSCK REPAIR TABLE ads.fn_ads_img_vector_arc;
show partitions ads.fn_ads_img_snapshot_arc;
show partitions ads.fn_ads_img_vector_arc;

hadoop distcp -Dmapreduce.map.memory.mb=8096 -m 40 -overwrite  hdfs://ha-nn-uri/user/hive/warehouse/ads.db/fn_ads_img_vector  s3://bigdata-offline/warehouse/ads/fn_ads_img_vector
MSCK REPAIR TABLE ads.fn_ads_img_vector;

