create external table if  not exists  ads.ads_vova_order_email (
    `email`                    string COMMENT 'd_邮箱',
    `region_name_cn`           string COMMENT 'i_国家',
    `language_code`            STRING COMMENT 'i_语言'
) COMMENT 'vova每周支付订单email' PARTITIONED BY (pt string)
     STORED AS PARQUETFILE;


alter table ads.ads_vova_order_email add columns(order_cnt string COMMENT 'i_八周订单数') cascade;