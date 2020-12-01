insert overwrite table dwb.dwb_fd_realtime_rpt partition(pt='${pt}',class='sessions')

select
    nvl(project,'all'),
    nvl(platform,'all'),
    nvl(country,'all'),
    count(distinct if(hour=0,session_id,null)) as h0,
    count(distinct if(hour=1,session_id,null)) as h1,
    count(distinct if(hour=2,session_id,null)) as h2,
    count(distinct if(hour=3,session_id,null)) as h3,
    count(distinct if(hour=4,session_id,null)) as h4,
    count(distinct if(hour=5,session_id,null)) as h5,
    count(distinct if(hour=6,session_id,null)) as h6,
    count(distinct if(hour=7,session_id,null)) as h7,
    count(distinct if(hour=8,session_id,null)) as h8,
    count(distinct if(hour=9,session_id,null)) as h9,
    count(distinct if(hour=10,session_id,null)) as h10,
    count(distinct if(hour=11,session_id,null)) as h11,
    count(distinct if(hour=12,session_id,null)) as h12,
    count(distinct if(hour=13,session_id,null)) as h13,
    count(distinct if(hour=14,session_id,null)) as h14,
    count(distinct if(hour=15,session_id,null)) as h15,
    count(distinct if(hour=16,session_id,null)) as h16,
    count(distinct if(hour=17,session_id,null)) as h17,
    count(distinct if(hour=18,session_id,null)) as h18,
    count(distinct if(hour=19,session_id,null)) as h19,
    count(distinct if(hour=20,session_id,null)) as h20,
    count(distinct if(hour=21,session_id,null)) as h21,
    count(distinct if(hour=22,session_id,null)) as h22,
    count(distinct if(hour=23,session_id,null)) as h23
from
(select
    date(derived_ts) as deriver_time,
    hour(derived_ts)  as hour,
    project,
    CASE platform_type
        WHEN 'pc_web' THEN 'PC'
        WHEN 'mobile_web' THEN 'H5'
        WHEN 'android_app' THEN 'Android'
        WHEN 'ios_app' THEN 'IOS'
        WHEN 'tablet_web' THEN 'Tablet'
        ELSE 'Others'
        END           as platform,
       country,
       session_id
from ods_fd_snowplow.ods_fd_snowplow_all_event
where pt='${pt}'
)tab1
group by  project,platform,country with cube;
