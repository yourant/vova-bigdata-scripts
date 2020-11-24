CREATE table if not exists dwb.dwb_fd_banner_ctr_rpt(
       project string,
       platform string,
       country string,
       app_version string,
       list_name string,
       element_name string,
       absolute_position bigint,
       click_session_id string,
       impression_session_id string
)comment 'list_type中含有banner的打点明细表'
partitioned by (pt string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS ORC
TBLPROPERTIES ("orc.compress"="SNAPPY");


insert overwrite table dwb.dwb_fd_banner_ctr_rpt partition (pt='${hiveconf:pt}')
select project,
       platform,
       country,
       app_version,
       list_type,
       element_name,
       absolute_position,
       click_session_id,
       impression_session_id
from (
         select project,
                platform,
                country,
                app_version,
                element_event_struct.list_type                               as list_type,
                element_event_struct.element_name,
                element_event_struct.absolute_position,
                if(event_name = 'common_click', session_id, null)      as click_session_id,
                if(event_name = 'common_impression', session_id, null) as impression_session_id
         from ods_fd_snowplow.ods_fd_snowplow_element_event 
         where pt = '${hiveconf:pt}'
           and lower(page_code) = 'homepage'
           and event_name in ('common_click', 'common_impression')
     ) tab1
where list_type regexp 'banner';
