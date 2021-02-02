insert overwrite table ads.ads_fd_120_position_impression_uv_7d_avg partition (pt = '${pt}')
select
    /*+ REPARTITION(2) */
    project_name ,
    platform_name ,
    route_sn ,
    route_name ,
    country ,
    absolute_position,
    cast(impression_uv as decimal(16,4))/cast(7 as decimal(15,4)) as impression_uv_avg,
    report_time as data_time
from dwb.dwb_fd_120_position_impression_uv_7d;
