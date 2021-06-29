
--打点数据40天的相关数据
with snowplow_view_40day as (
select
    domain_userid,
    session_id,
    mkt_source,
    mkt_campaign,
    mkt_term,
    mkt_content,
    mkt_medium,
    mkt_clickid,
    mkt_network,
    page_code,
    country,
    language,
    platform_type as platform,
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
    dvce_type,
    geo_country,
    page_url as url
from
    ods_fd_snowplow.ods_fd_snowplow_view_event
where
    pt BETWEEN date_sub('2021-05-31', 40) AND '2021-05-31'
),

--订单表和session之间的关联表
ods_fd_order_marketing_data_distinct as (
select order_id, sp_session_id from ods_fd_vb.ods_fd_order_marketing_data group by order_id, sp_session_id
),

--下单商品订单ID表
order_info_40day as (
select
    order_id
from dwd.dwd_fd_order_info
where
    date(from_unixtime(order_time, 'yyyy-MM-dd hh:mm:ss')) = '2021-05-31'
    --and email NOT REGEXP "tetx.com|i9i8.com|jjshouse.com|jenjenhouse.com|163.com|qq.com"
group by order_id
),

--所有已支付订单的domain_userid来源
order_domain_userid_info as (
select
    fms.domain_userid
from
    order_info_40day oi
inner join
    ods_fd_order_marketing_data_distinct om
on
    om.order_id = oi.order_id

inner join
    (select session_id,domain_userid from snowplow_view_40day group by session_id,domain_userid) fms
on
    om.sp_session_id = fms.session_id
)

insert overwrite table dwd.dwd_fd_order_hit partition (pt = '2021-05-31')
select
    sv.domain_userid,
    sv.session_id,
    sv.mkt_source,
    sv.mkt_campaign,
    sv.mkt_term,
    sv.mkt_content,
    sv.mkt_medium,
    sv.mkt_clickid,
    sv.mkt_network,
    sv.page_code,
    sv.country,
    sv.language,
    sv.platform,
    sv.os,
    sv.os_family,
    sv.os_version,
    sv.dvce_type,
    sv.geo_country,
    sv.url
from
    snowplow_view_40day sv
inner join
    order_domain_userid_info odui
on
    sv.domain_userid = odui.domain_userid
;

insert overwrite table ods_fd_vb. ods_fd_order_extension_h
select * from ods_fd_vb. ods_fd_order_extension;