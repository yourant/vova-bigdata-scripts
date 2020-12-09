CREATE table if not exists dwb.dwb_fd_banner_ctr_rpt(
       project string,
       platform string,
       country string,
       app_version string,
       dvce_type string,
       list_name string,
       element_name string,
       absolute_position string,
       click_uv bigint,
       impression_uv bigint
)comment 'list_type中含有banner的打点明细表'
partitioned by (pt string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS ORC
TBLPROPERTIES ("orc.compress"="SNAPPY");