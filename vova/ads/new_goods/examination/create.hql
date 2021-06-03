DROP TABLE ads.ads_vova_new_goods_merchant_block_list;
CREATE EXTERNAL TABLE IF NOT EXISTS ads.ads_vova_new_goods_merchant_block_list
(
    mct_id           bigint,
    mct_name         bigint
) COMMENT 'ads_vova_new_goods_merchant_block_list'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

INSERT OVERWRITE TABLE ads.ads_vova_new_goods_merchant_block_list
SELECT dm.mct_id,
        dm.mct_name
FROM dim.dim_vova_merchant dm
WHERE dm.mct_name IN
      ('XUOIHcas', 'jasons', 'hgfhjfgj', 'sfgsfgs', 'HDUYH', 'cjfuq', 'r836417 Mall', 'Eiffel department store',
       'Pada-lL', 'Shop 1', 'DWBJI', 'peuiy', 'zhengtiwu', 'XOXO');

--init new_goods_merchant_block_list_poll
INSERT OVERWRITE TABLE ads.ads_vova_goods_examination_poll_arc PARTITION (pt = '2020-12-12')
select
poll.goods_id,
poll.goods_source_image,
poll.goods_source_basic,
poll.goods_source_text,
poll.add_test_time
from
ads.ads_vova_goods_examination_poll poll
INNER JOIN dwd.dim_goods dg ON dg.goods_id = poll.goods_id
LEFT JOIN ads.ads_vova_new_goods_merchant_block_list bl on bl.mct_id = dg.mct_id
where bl.mct_id is null
;

DROP TABLE ads.ads_vova_goods_examination_poll_inc;
CREATE EXTERNAL TABLE IF NOT EXISTS ads.ads_vova_goods_examination_poll_inc
(
    goods_id           bigint,
    goods_source_image bigint,
    goods_source_basic bigint,
    goods_source_text  bigint,
    add_test_time      TIMESTAMP
) COMMENT 'ads_vova_goods_examination_poll_inc' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

DROP TABLE ads.ads_vova_goods_examination_poll_arc;
CREATE EXTERNAL TABLE IF NOT EXISTS ads.ads_vova_goods_examination_poll_arc
(
    goods_id           bigint,
    goods_source_image bigint,
    goods_source_basic bigint,
    goods_source_text  bigint,
    add_test_time      TIMESTAMP
) COMMENT 'ads_vova_goods_examination_poll_arc' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

DROP TABLE ads.ads_vova_goods_examination_poll;
CREATE EXTERNAL TABLE IF NOT EXISTS ads.ads_vova_goods_examination_poll
(
    goods_id           bigint,
    goods_source_image bigint,
    goods_source_basic bigint,
    goods_source_text  bigint,
    add_test_time      TIMESTAMP
) COMMENT 'ads_vova_goods_examination_poll'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

select count(*),count(goods_id),original_name,pt from ads.ads_vova_goods_test_collector_data group by pt,original_name order by pt
DROP TABLE ads.ads_vova_goods_test_collector_data;
CREATE EXTERNAL TABLE IF NOT EXISTS ads.ads_vova_goods_test_collector_data
(
    goods_id         bigint,
    original_name    string,
    datasource       string,
    platform         string,
    device_id        string,
    collector_tstamp string
) COMMENT 'ads_vova_goods_test_collector_data' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

DROP TABLE ads.ads_vova_new_goods_examination_behave;
CREATE EXTERNAL TABLE IF NOT EXISTS ads.ads_vova_new_goods_examination_behave
(
    datasource         string,
    goods_id           bigint,
    impressions        bigint,
    impressions_uv     bigint,
    clicks             bigint,
    clicks_uv          bigint,
    add_cart_cnt       bigint,
    gmv                decimal(20, 4),
    sales_order        bigint,
    ctr                decimal(20, 6),
    gcr                decimal(20, 6),
    gmv_cr             decimal(20, 6),
    goods_score        decimal(20, 6)
) COMMENT 'ads_vova_new_goods_examination_behave'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;


DROP TABLE ads.ads_vova_new_goods_examination_summary;
CREATE EXTERNAL TABLE IF NOT EXISTS ads.ads_vova_new_goods_examination_summary
(
    datasource         string,
    goods_id           bigint,
    impressions        bigint,
    ctr                decimal(20, 6),
    gcr                decimal(20, 6),
    gmv_cr             decimal(20, 6),
    goods_score        decimal(20, 6),
    test_goods_status  bigint,
    test_goods_result_status  bigint,
    status_change_time TIMESTAMP,
    add_test_time TIMESTAMP
) COMMENT 'ads_vova_new_goods_examination_summary'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

DROP TABLE ads.ads_vova_new_goods_examination_summary_history;
CREATE EXTERNAL TABLE IF NOT EXISTS ads.ads_vova_new_goods_examination_summary_history
(
    datasource         string,
    goods_id           bigint,
    impressions        bigint,
    ctr                decimal(20, 6),
    gcr                decimal(20, 6),
    gmv_cr             decimal(20, 6),
    goods_score        decimal(20, 6),
    test_goods_status  bigint,
    test_goods_result_status  bigint,
    status_change_time TIMESTAMP,
    add_test_time TIMESTAMP
) COMMENT 'ads_vova_new_goods_examination_summary_history' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

DROP TABLE ads.ads_vova_new_goods_examination_summary_history_export;
CREATE EXTERNAL TABLE IF NOT EXISTS ads.ads_vova_new_goods_examination_summary_history_export
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
    status_change_time TIMESTAMP,
    goods_source_image bigint,
    goods_source_basic bigint,
    goods_source_text bigint
) COMMENT 'ads_vova_new_goods_examination_summary_history_export' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

DROP TABLE ads.ads_vova_new_goods_examination_summary_display;
CREATE EXTERNAL TABLE IF NOT EXISTS ads.ads_vova_new_goods_examination_summary_display
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
    status_change_time TIMESTAMP,
    goods_source_image bigint,
    goods_source_basic bigint,
    goods_source_text bigint,
    rec_original string
) COMMENT 'ads_vova_new_goods_examination_summary_history_export'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

DROP TABLE dwb.dwb_vova_new_goods_examination;
CREATE EXTERNAL TABLE IF NOT EXISTS dwb.dwb_vova_new_goods_examination
(
    event_date                date,
    rec_original              string,
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
) COMMENT 'dwb_vova_new_goods_examination' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

-- from_unixtime(unix_timestamp(date_add('${cur_date}', 1), 'yyyy-MM-dd'))        AS add_test_time

