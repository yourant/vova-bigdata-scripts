
CREATE table if not exists  dwb.dwb_fd_page_data_rpt
(
    project string comment'组织',
    country string comment'国家',
    platform_type string comment'平台具体类型 ios_app/tablet_web/pc_web/android_app/mobile_web/others',
    os_name string COMMENT 'os系统名及版本',
    app_version string comment 'app版本',
    is_new_user string comment '是否新用户',
    page_code string comment '页面代码',
    view_pv       bigint comment'页面pv',
    view_uv       bigint comment'页面uv'
)comment '打点数据页面浏览量的uv,pv报表'
partitioned by(`pt` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS orc
TBLPROPERTIES ("orc.compress"="SNAPPY");


     platform string comment'平台mob/web',
    dvce_type string comment'设备类型，Computer/Mobile/Tablet',