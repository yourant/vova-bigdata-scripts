insert overwrite table tmp.tmp_fd_order_info_h
select
/*+ REPARTITION(10) */
order_id
from ods_fd_vb.ods_fd_order_info_h
where date(from_utc_timestamp(to_utc_timestamp(order_time,'GMT-7'),"PRC")) >= '${pt_begin}'
and date(from_utc_timestamp(to_utc_timestamp(order_time,'GMT-7'),"PRC")) <= '${pt_end}';

insert overwrite table tmp.tmp_fd_order_extension_h
select
/*+ REPARTITION(10) */
order_id,ext_value from ods_fd_vb.ods_fd_order_extension_h where ext_name='durid';

--打点数据40天的相关数据
with tmp_snowplow_view_40day as (
select
/*+ REPARTITION(20) */
collector_ts,
domain_userid,
trim(lower(project)) as project,
session_id,
mkt_source,
mkt_campaign,
mkt_term,
mkt_content,
mkt_medium,
mkt_clickid as mkt_click_id,
mkt_network,
page_code,
country,
language,
case when platform_type like '%web%' then 'web'
when platform_type like '%app%' then 'mob' else '' end as platform,
os_name as os,
case when os_name like '%Android%' then 'Android'
when os_name like '%iOS%' then 'iOS'
when os_name like '%Windows%' then 'Windows'
when os_name like '%Linux%' then 'Linux'
when os_name like '%Mac OS X%' then 'Mac OS X'
when os_name like '%Chrome OS%' then 'Chrome OS'
when os_name like '%Ubuntu%' then 'Ubuntu'
when os_name like '%Unknown mobile%' then 'Unknown mobile'
when os_name like '%Unknown tablet%' then 'Unknown tablet'
when os_name like '%Unknown%' then 'Unknown'
when os_name like '%Proxy%' then 'Proxy'
when os_name like '%BlackBerryOS%' then 'BlackBerryOS'
when os_name like '%Sony Playstation%' then 'Sony Playstation'
when os_name like '%Xbox OS%' then 'Xbox OS'
when os_name like '%BlackBerry Tablet OS%' then 'BlackBerry Tablet OS'
when os_name like '%BlackBerry%' then 'BlackBerry'
when os_name like '%Sony Ericsson%' then 'Sony Ericsson'
when os_name like '%Mac OS%' then 'Mac OS'
when os_name like '%BlackBerry%' then 'BlackBerry'
when os_name like '%Other%' then 'Other'
else os_name end as os_family,
os_version,
dvce_type as device_type,
geo_country,
case when platform_type like '%web%' then page_url
when platform_type like '%app%' then referrer_url else '' end as url
from
ods_fd_snowplow.ods_fd_snowplow_view_event
where
pt BETWEEN date_sub('${pt_end}', 44) AND '${pt_end}' and length(country) < 3
),

goods_info as (
select
/*+ REPARTITION(10) */
g.goods_id, g.cat_id, vg.virtual_goods_id, lower(vg.project_name) as project_name
from ods_fd_vb.ods_fd_goods_h g
inner join ods_fd_vb.ods_fd_virtual_goods_h vg on vg.goods_id = g.goods_id
),

tmp_fd_order_info as (
select
/*+ REPARTITION(10) */
order_id
from tmp.tmp_fd_order_info_h group by order_id
),

tmp_fd_order_extension as (
select
/*+ REPARTITION(10) */
order_id,
ext_value
from tmp.tmp_fd_order_extension_h group by order_id,ext_value
),

order_domain_userid_info_tmp as (
select
/*+ REPARTITION(10) */
oe.ext_value as domain_userid
from
    tmp_fd_order_info oi
inner join
    tmp_fd_order_extension oe
on
    trim(oi.order_id) = trim(oe.order_id)
),

--所有已支付订单的domain_userid来源
order_domain_userid_info as (
select
/*+ REPARTITION(5) */
domain_userid
from
order_domain_userid_info_tmp
group by
domain_userid
)

insert overwrite table ads.ads_fd_order_hit
select
/*+ REPARTITION(20) */
    sv.collector_ts,
    sv.domain_userid,
    sv.session_id,
    sv.mkt_source,
    sv.mkt_campaign,
    sv.mkt_term,
    sv.mkt_content,
    sv.mkt_medium,
    sv.mkt_click_id,
    sv.mkt_network,
    sv.page_code,
    sv.country,
    sv.language,
    sv.platform,
    sv.os,
    sv.os_family,
    sv.os_version,
    sv.device_type,
    sv.geo_country,
    sv.url
from
    tmp_snowplow_view_40day sv
left join goods_info gi on gi.virtual_goods_id = sv.url_virtual_goods_id and project=project_name
inner join
    order_domain_userid_info odui
on
    sv.domain_userid = odui.domain_userid
;

