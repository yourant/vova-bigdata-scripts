CREATE table if not exists  dwb.dwb_fd_common_ctr_rpt
(
    platform_type string,
    country string,
    project string,
    page_code string,
    list_name string,
    element_name string,
    impression_uv bigint,
    click_uv  bigint
)comment '打点数据common_event的ctr报表'
partitioned by(`pt` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS parquet
TBLPROPERTIES ("parquet.compress"="SNAPPY");