CREATE TABLE IF NOT EXISTS dwb.dwb_fd_abtest_funnel_rate_rpt
(
    project                    string,
    platform_type              string,
    country                    string,
    app_version                string,
    abtest_name                string,
    abtest_version             string,
    uv                         bigint,
    product_uv                 bigint,
    add_uv                     bigint,
    checkout_uv                bigint,
    checkout_option_uv         bigint,
    purchase_uv                bigint
) comment'utc时间每天的abtest打点转化明细表'
    PARTITIONED BY ( pt string)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
    STORED AS parquet
    TBLPROPERTIES ("parquet.compress"="SNAPPY");