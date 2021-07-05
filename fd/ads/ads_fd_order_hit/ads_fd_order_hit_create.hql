create table if not exists ads.ads_fd_order_hit (
    `timestamp`                            timestamp COMMENT '事件时间',
    goods_id                               bigint COMMENT '商品ID',
    domain_userid                          string COMMENT 'domain 用户ID',
    session_id                             string COMMENT 'sessionID',
    mkt_source                             string COMMENT '',
    mkt_campaign                           string COMMENT '',
    mkt_term                               string COMMENT '',
    mkt_content                            string COMMENT '',
    mkt_medium                             string COMMENT '',
    mkt_click_id                           string COMMENT '',
    mkt_network                            string COMMENT '',
    page_code                              string COMMENT '页面标识',
    country                                string COMMENT '国家',
    language                               string COMMENT '语言',
    platform                               string COMMENT '平台',
    os                                     string COMMENT '系统名字',
    os_family                              string COMMENT '操作系统',
    os_version                             string COMMENT '系统版本',
    device_type                            string COMMENT '设备类型',
    geo_country                            string COMMENT 'geo国家',
    url                                    string COMMENT 'URL链接'
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;
