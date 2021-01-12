drop table dwd.dwd_vova_fact_cart_cause_v2;
CREATE external TABLE IF NOT EXISTS dwd.dwd_vova_fact_cart_cause_v2
(
    datasource          string,
    event_name          string,
    virtual_goods_id    string,
    device_id           string,
    buyer_id            bigint,
    platform            string,
    country             string,
    referrer            string,
    dvce_created_tstamp bigint,
    pre_page_code       string,
    pre_list_type       string,
    pre_list_uri        string,
    pre_element_type    string,
    pre_app_version     string,
    pre_test_info       string,
    pre_recall_pool     string,
    pre_position     string
) COMMENT '加车归因' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

