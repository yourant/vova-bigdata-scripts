SET hive.exec.compress.output=true;

with fd_goods_session_today as (
select session_id from dwd.dwd_fd_goods_snowplow where pt='${pt_end}' group by session_id
),
fd_goods_session_last_day as (
select session_id from dwd.dwd_fd_goods_snowplow where pt='${pt_begin}' group by session_id
),

fd_session_intersection as (
select ld.session_id from fd_goods_session_last_day ld inner join fd_goods_session_today td on ld.session_id = td.session_id
),

fd_goods_snowplow_need as (
select
    gs.*,
    nvl(si.session_id, 'yes') as mark
from
    (select * from dwd.dwd_fd_goods_snowplow where record_type != 'order' and pt = '${pt_end}')  gs
left join
    fd_session_intersection si
on
    gs.session_id = si.session_id
),

insert overwrite table dwb.dwb_fd_goods_snowplow_uv partition (pt = '${pt_end}')
select
    /*+ REPARTITION(20) */
    record_type,
    trim(lower(project)) as project,
    platform_type,
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
    fd_goods_snowplow_need
where mark = 'yes'
group by
    record_type,
    trim(lower(project)),
    platform_type,
    trim(upper(country)),
    trim(lower(language)),
    cat_id,
    goods_id,
    page_code,
    list_type
;



