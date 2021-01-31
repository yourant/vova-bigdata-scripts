create external table if not exists ads.ads.ads_fd_druid_goods_event
(
    event_time        string,
    record_type       string,
    project           string,
    domain_userid     string,
    session_id        string,
    platform_type     string,
    country           string,
    cat_id            string,
    goods_id          string,
    virtual_goods_id  string,
    page_code         string,
    list_type         string,
    absolute_position bigint,
    url_route_sn      string,
    event_num         bigint,
    order_num         bigint,
    paying_order_num  bigint,
    paid_order_num    bigint
) PARTITIONED BY (
    `pt` string,
    `hour` string)
    ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
    stored as textfile;