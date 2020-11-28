
CREATE table if not exists  dwb.dwb_fd_page_data_rpt
(
    project string,
    platform string,
    platform_type string,
    dvce_type string,
    os_name string,
    country string,
    app_version string,
    is_new_user string,
    page_code string,
    session_id string
)comment '打点数据页面浏览量的ctr报表'
partitioned by(`pt` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS orc
TBLPROPERTIES ("orc.compress"="SNAPPY");