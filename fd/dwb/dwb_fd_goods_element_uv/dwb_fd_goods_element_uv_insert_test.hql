SET hive.exec.compress.output=true;

with fd_goods_session_today as (
select session_id from ods_fd_snowplow.ods_fd_snowplow_all_event where pt='2021-07-01' and event_name in ("common_click", "common_impression") group by session_id
),
fd_goods_session_last_day as (
select session_id from ods_fd_snowplow.ods_fd_snowplow_all_event where pt='2021-06-30' and event_name in ("common_click", "common_impression") group by session_id
),

fd_session_intersection as (
select ld.session_id from fd_goods_session_last_day ld inner join fd_goods_session_today td on ld.session_id = td.session_id
),

fd_goods_snowplow_need as (
select
    gs.*,
    nvl(si.session_id, 'yes') as mark
from
    (select * from ods_fd_snowplow.ods_fd_snowplow_all_event where pt = '2021-07-01' and event_name in ("common_click", "common_impression"))  gs
left join
    fd_session_intersection si
on
    gs.session_id = si.session_id
),
fd_goods_element_snowplow as (
--impression
SELECT
/*+ REPARTITION(10) */
    ge.virtual_goods_id as goods_id,
    trim(lower(ge.project)) as project,
    if(ge.platform_type='mobile_web', 'h5',if(ge.platform_type in ('android_app', 'ios_app'),'mob',if(ge.platform_type in ('tablet_web','pc_web'),'web','other'))) as platform,
    trim(upper(ge.country)) as country,
    case ge.event_name
        when "common_click" then "click"
        when "common_impression" then "impression"
        end as rtype,
    ge.page_code,
    ge.element_name,
    ge.element_tag,
    ge.element_batch,
    ge.session_id
from
    (select
    *,
    case when ee.types='goods' then ee.element_id
    else regexp_extract(page_url,'(m|/|mid=)([0-9]{5,10})',2) end as virtual_goods_id,
    ee.element_name,
    ee.element_tag,
    ee.element_batch
    from
    fd_goods_snowplow_need LATERAL VIEW OUTER explode(element_event_struct) element_event as ee
    where mark='yes') ge
)

insert overwrite table dwb.dwb_fd_goods_element_uv partition (pt = '2021-07-01')
select
    /*+ REPARTITION(10) */
    goods_id,
    project,
    platform,
    country,
    rtype,
    page_code,
    element_name,
    element_tag,
    element_batch,
    count(distinct session_id) as uv
from
    fd_goods_element_snowplow
group by
    goods_id,
    project,
    platform,
    country,
    rtype,
    page_code,
    element_name,
    element_tag,
    element_batch;
