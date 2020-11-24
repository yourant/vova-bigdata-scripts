use dwb;

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
    session_id string,
)comment '打点数据页面浏览量的ctr报表'
partitioned by(`pt` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS orc
TBLPROPERTIES ("orc.compress"="SNAPPY");



insert overwrite table  dwb.dwb_fd_page_data_rpt(partition pt='${hiveconf:pt}')
as select
       project,
       platform,
       platform_type,
       dvce_type,
       os_name,
       country,
       app_version,
       case
            when session_idx=1 then 'new' 
            when session_idx>1 then 'old'
            end  as is_new_user,
       page_code,
       session_id
from ods_fd_snowplow.ods_fd_snowplow_all_event
where event_name in('page_view','screen_view');
