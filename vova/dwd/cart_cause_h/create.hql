drop table dwd.dwd_vova_fact_cart_cause_h;
CREATE external TABLE IF NOT EXISTS dwd.dwd_vova_fact_cart_cause_h
(
    datasource            string comment '数据平台',
    event_name            string comment '事件名称',
    virtual_goods_id      string comment '虚拟商品id',
    device_id             string comment '设备id',
    buyer_id              bigint comment '买家id',
    platform              string comment '数据平台',
    country               string comment '国家',
    referrer              string comment 'referrer',
    dvce_created_tstamp   string comment '事件创建时间',
    collector_tstamp      string comment '事件接收时间',
    pre_page_code         string comment '归因页面(最后一次点击页面)',
    pre_list_type         string comment '归因list_type(最后一次点击list_type)',
    pre_list_uri          string comment '归因list_uri(最后一次点击list_uri)',
    pre_element_type      string comment '归因element_type(最后一次点击element_type)',
    pre_app_version       string comment '归因app_version(最后一次点击app_version)',
    pre_test_info         string comment '归因test_info(最后一次点击test_info)'
) COMMENT '加车小时归因' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;