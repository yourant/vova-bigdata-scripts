create external table dwd.dwd_fd_goods_snowplow_performance
(
    project          STRING,
    country          STRING,
    language         STRING,
    platform_type    STRING,
    platform_name    STRING,
    event_name       STRING,
    page_code        STRING,
    goods_event_name STRING,
    goods_id         int,
    virtual_goods_id int,
    session_id       STRING
) partitioned by (pt string)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
    stored as parquet;
