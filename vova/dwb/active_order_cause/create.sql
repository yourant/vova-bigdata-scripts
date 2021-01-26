drop table if exists dwb.dwb_rec_active_report_rate_analysis;
CREATE EXTERNAL TABLE IF NOT EXISTS dwb.dwb_rec_active_report_rate_analysis
(
    event_date              STRING  COMMENT 'd_日期',
    datasource              STRING  COMMENT 'd_datasource',
    country                 STRING  COMMENT 'd_country',
    os_type                 STRING  COMMENT 'd_os_type',
    page_code               STRING  COMMENT 'd_page_code',
    list_type               STRING  COMMENT 'd_list_type',
    activate_time           STRING  COMMENT 'd_activate_time',
    active_page_income_rate STRING COMMENT 'i_active_page_income_rate',
    goods_view_rate         STRING COMMENT 'i_goods_view_rate',
    try_cart_rate           STRING COMMENT 'i_try_cart_rate',
    cart_rate               STRING COMMENT 'i_cart_rate',
    cart_change_rate        STRING COMMENT 'i_cart_change_rate',
    order_rate              STRING COMMENT 'i_order_rate',
    pay_success_rate        STRING COMMENT 'i_pay_success_rate',
    total_change_rate       STRING COMMENT 'i_total_change_rate',
    element_type            STRING COMMENT 'i_element_type'
)
    COMMENT 'rate_analysis'
    PARTITIONED BY (pt STRING)  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;