SET hive.exec.compress.output=true;

with ads_fd_druid_goods_week as (
select 
    virtual_goods_id as goods_id,
    cat_id,
    country,
    project,
    session_id,
    record_type,
    list_type,
    if(platform_type='mobile_web', 'H5',if(platform_type in ('android_app', 'ios_app'),'app',if(platform_type in ('tablet_web','pc_web'),'PC','other'))) as platform_type,
    event_time,
    page_code,
    paying_order_num
from ads.ads_fd_goods_event
where pt >= '${pt_begin}' and pt <= '${pt_end}'
)

insert overwrite table tmp.tmp_fd_goods_event_week
select
/*+ REPARTITION(20) */
    goods_id,
    cat_id,
    country,
    project,
    session_id,
    record_type,
    list_type,
    platform_type,
    page_code,
    paying_order_num
from
ads_fd_druid_goods_week
where event_time >= '${time_begin}' and event_time < '${time_end}' and length(country) < 3 and goods_id is not null and goods_id != '';
