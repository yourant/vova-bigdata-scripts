SET hive.exec.compress.output=true;

with fd_goods_session_today as (
select session_id from dwd.dwd_fd_goods_snowplow where pt='${pt_end}' and hour < '16' group by session_id
),
fd_goods_session_last_day as (
select session_id from dwd.dwd_fd_goods_snowplow where pt='${pt_two}' group by session_id
),

fd_session_intersection as (
select ld.session_id from fd_goods_session_last_day ld inner join fd_goods_session_today td on ld.session_id = td.session_id
),

fd_goods_snowplow_need as (
select
    gs.*,
    nvl(si.session_id, 'yes') as mark
from
    (select * from dwd.dwd_fd_goods_snowplow where record_type != 'order' and pt = '${pt_end}'  and hour < '16')  gs
left join
    fd_session_intersection si
on
    gs.session_id = si.session_id
),


tmp_fd_goods_uv_need as (
select
    /*+ REPARTITION(20) */
    record_type,
    trim(lower(project)) as project,
    if(platform_type='mobile_web', 'h5',if(platform_type in ('android_app', 'ios_app'),'mob',if(platform_type in ('tablet_web','pc_web'),'web','other'))) as platform,
    trim(upper(country)) as country,
    trim(lower(language)) as language,
    cat_id,
    goods_id,
    page_code,
    list_type,
    count(distinct session_id) as goods_uv
from
    fd_goods_snowplow_need
where mark = 'yes'
group by
    record_type,
    trim(lower(project)),
    if(platform_type='mobile_web', 'h5',if(platform_type in ('android_app', 'ios_app'),'mob',if(platform_type in ('tablet_web','pc_web'),'web','other'))),
    trim(upper(country)),
    trim(lower(language)),
    cat_id,
    goods_id,
    page_code,
    list_type

union all

select
    /*+ REPARTITION(20) */
    record_type,
    trim(lower(project)) as project,
    if(platform_type='mobile_web', 'h5',if(platform_type in ('android_app', 'ios_app'),'mob',if(platform_type in ('tablet_web','pc_web'),'web','other'))) as platform,
    trim(upper(country)) as country,
    trim(lower(language)) as language,
    cat_id,
    goods_id,
    page_code,
    list_type,
    count(distinct session_id) as goods_uv,
    sum(event_num)                             as event_num,
    sum(order_num)                             as order_num,
    sum(paying_order_num)                      as paying_order_num,
    sum(paid_order_num)                        as paid_order_num
from
    dwd.dwd_fd_goods_snowplow
where record_type != 'order' and pt = '${pt_begin}'  and hour > '15'
group by
    record_type,
    trim(lower(project)),
    if(platform_type='mobile_web', 'h5',if(platform_type in ('android_app', 'ios_app'),'mob',if(platform_type in ('tablet_web','pc_web'),'web','other'))),
    trim(upper(country)),
    trim(lower(language)),
    cat_id,
    goods_id,
    page_code,
    list_type


union all

select
/*+ REPARTITION(20) */
    record_type,
    project,
    if(platform_type='mobile_web', 'h5',if(platform_type in ('android_app', 'ios_app'),'mob',if(platform_type in ('tablet_web','pc_web'),'web','other'))) as platform,
    country,
    language,
    cat_id,
    goods_id,
    page_code,
    list_type,
    goods_uv,
    event_num,
    order_num,
    paying_order_num,
    paid_order_num
from
    dwb.dwb_fd_goods_snowplow_uv
where pt >= '${pt_one}' and pt <= '${pt_two}'
)

insert overwrite table tmp.tmp_fd_goods_uv_interval
select
    /*+ REPARTITION(20) */
    goods_id,
    cat_id,
    record_type,
    project,
    country,
    language,
    platform,
    page_code,
    list_type,
    goods_uv,
    event_num,
    order_num,
    paying_order_num,
    paid_order_num
from
tmp_fd_goods_uv_need where goods_id is not null
;
