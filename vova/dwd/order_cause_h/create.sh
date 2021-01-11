drop table dwd.dwd_vova_fact_order_cause_h;
CREATE TABLE IF NOT EXISTS dwd.dwd_vova_fact_order_cause_h
(
    datasource            string comment '数据平台',
    goods_id              bigint comment '商品id',
    device_id             string comment '设备id',
    buyer_id              bigint comment '买家id',
    order_goods_id        bigint comment '子订单id',
    platform              string comment '平台',
    pre_page_code         string comment '归因页面(最后一次点击页面)',
    pre_list_type         string comment '归因list_type(最后一次点击list_type)',
    pre_list_uri          string comment '归因list_uri(最后一次点击list_uri)',
    pre_element_type      string comment '归因element_type(最后一次点击element_type)',
    pre_app_version       string comment '归因app_version(最后一次点击app_version)',
    pre_test_info       string comment '归因test_info(test_info)',
    pre_recall_pool       string comment '归因recall_pool(recall_pool)'
) COMMENT '订单小时归因' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

