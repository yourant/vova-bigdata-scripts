DROP TABLE ads.ads_vova_web_examination_pre;
CREATE EXTERNAL TABLE IF NOT EXISTS ads.ads_vova_web_examination_pre
(
    datasource         string,
    goods_id           bigint,
    impressions        bigint,
    clicks             bigint,
    clicks_uv          bigint,
    sales_order        bigint,
    gmv                decimal(20, 2),
    add_cart_cnt       bigint,
    ctr                decimal(20, 6),
    rate               decimal(20, 6),
    gr                 decimal(20, 6),
    gcr                decimal(20, 6),
    gmv_cr             decimal(20, 6),
    goods_score        decimal(20, 6)
) COMMENT 'ads_vova_web_examination_pre' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

DROP TABLE ads.ads_vova_web_examination_poll_arc;
CREATE EXTERNAL TABLE IF NOT EXISTS ads.ads_vova_web_examination_poll_arc
(
    datasource         string,
    goods_id           bigint,
    add_test_time      TIMESTAMP
) COMMENT 'ads_vova_web_examination_poll_arc' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

DROP TABLE ads.ads_vova_web_examination_poll;
CREATE EXTERNAL TABLE IF NOT EXISTS ads.ads_vova_web_examination_poll
(
    datasource         string,
    goods_id           bigint,
    add_test_time      TIMESTAMP
) COMMENT 'ads_vova_web_examination_poll'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

DROP TABLE ads.ads_vova_web_goods_examination_collector_data;
CREATE EXTERNAL TABLE IF NOT EXISTS ads.ads_vova_web_goods_examination_collector_data
(
    goods_id         bigint,
    original_name    string,
    datasource       string,
    domain_userid    string,
    collector_ts     string
) COMMENT 'ads_vova_web_goods_examination_collector_data' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

DROP TABLE ads.ads_vova_web_goods_examination_behave;
CREATE EXTERNAL TABLE IF NOT EXISTS ads.ads_vova_web_goods_examination_behave
(
    datasource       string,
    goods_id         bigint,
    impressions      bigint,
    impressions_uv   bigint,
    clicks           bigint,
    clicks_uv        bigint,
    add_cart_cnt     bigint,
    gmv              decimal(20, 2),
    sales_order      bigint,
    ctr              decimal(20, 6),
    gcr              decimal(20, 6),
    gmv_cr           decimal(20, 6),
    goods_score      decimal(20, 6)
) COMMENT 'ads_vova_web_goods_examination_behave'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

DROP TABLE tmp.tmp_ads_vova_web_goods_examination_pre;
CREATE EXTERNAL TABLE IF NOT EXISTS tmp.tmp_ads_vova_web_goods_examination_pre
(
    datasource       string,
    goods_id         bigint,
    impressions      bigint,
    ctr              decimal(20, 6),
    gcr              decimal(20, 6),
    gmv_cr           decimal(20, 6),
    goods_score      decimal(20, 6),
    test_goods_status  bigint,
    test_goods_result_status  bigint,
    status_change_time  TIMESTAMP,
    add_test_time       TIMESTAMP
) COMMENT 'tmp_ads_vova_web_goods_examination_pre'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

DROP TABLE tmp.tmp_ads_vova_web_goods_examination_diff;
CREATE EXTERNAL TABLE IF NOT EXISTS tmp.tmp_ads_vova_web_goods_examination_diff
(
    datasource       string,
    goods_id         bigint,
    impressions      bigint,
    ctr              decimal(20, 6),
    gcr              decimal(20, 6),
    gmv_cr           decimal(20, 6),
    goods_score      decimal(20, 6),
    test_goods_status  bigint,
    test_goods_result_status  bigint,
    status_change_time  TIMESTAMP,
    add_test_time       TIMESTAMP
) COMMENT 'tmp_ads_vova_web_goods_examination_diff'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

DROP TABLE tmp.tmp_ads_vova_web_goods_examination_not_change;
CREATE EXTERNAL TABLE IF NOT EXISTS tmp.tmp_ads_vova_web_goods_examination_not_change
(
    datasource       string,
    goods_id         bigint,
    status_change_time  TIMESTAMP
) COMMENT 'tmp_ads_vova_web_goods_examination_not_change'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

DROP TABLE ads.ads_vova_web_goods_examination_summary;
CREATE EXTERNAL TABLE IF NOT EXISTS ads.ads_vova_web_goods_examination_summary
(
    datasource       string,
    goods_id         bigint,
    impressions      bigint,
    ctr              decimal(20, 6),
    gcr              decimal(20, 6),
    gmv_cr           decimal(20, 6),
    goods_score      decimal(20, 6),
    test_goods_status  bigint,
    test_goods_result_status  bigint,
    status_change_time  TIMESTAMP,
    add_test_time       TIMESTAMP
) COMMENT 'ads_vova_web_goods_examination_summary'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

DROP TABLE ads.ads_vova_web_goods_examination_summary_history_export;
CREATE EXTERNAL TABLE IF NOT EXISTS ads.ads_vova_web_goods_examination_summary_history_export
(
    datasource         STRING,
    goods_id           bigint,
    cat_id             bigint,
    first_cat_id       bigint,
    second_cat_id      bigint,
    impressions        bigint,
    ctr                decimal(20, 6),
    gcr                decimal(20, 6),
    gmv_cr             decimal(20, 6),
    goods_score        decimal(20, 6),
    gcr_1w             decimal(20, 6),
    gmv_cr_1w          decimal(20, 6),
    impressions_1w     bigint,
    test_goods_status  bigint,
    test_goods_status_comment  string,
    test_goods_result_status  bigint,
    test_goods_result_comment  string,
    add_test_time      TIMESTAMP,
    status_change_time TIMESTAMP
) COMMENT 'ads_vova_web_goods_examination_summary_history_export' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

DROP TABLE dwb.dwb_vova_web_goods_examination;
CREATE EXTERNAL TABLE IF NOT EXISTS dwb.dwb_vova_web_goods_examination
(
    event_date                date,
    first_level_testing_cnt   bigint,
    first_level_finished_cnt  bigint,
    second_level_finished_cnt bigint,
    third_level_finished_cnt  bigint,
    first_level_cnt           bigint,
    first_level_success_cnt   bigint,
    first_level_failure_cnt   bigint,
    second_level_success_cnt  bigint,
    second_level_failure_cnt  bigint,
    third_level_success_cnt   bigint,
    third_level_failure_cnt   bigint
) COMMENT 'dwb_vova_web_goods_examination' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

